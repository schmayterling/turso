//! Parsing with libpg_query, with a bound on the depth of the parse tree.
//!
//! `pg_query::parse` decodes the protobuf parse tree with prost, and the
//! decode recurses one time for each nested message, with large stack frames.
//! prost's own limit of 100 nested messages refuses queries that PostgreSQL
//! runs, such as 16 nested subqueries, so it is turned off, and this module
//! counts the depth on the protobuf bytes before the decode. pg_query keeps
//! its bindings private, so this module calls libpg_query itself.

use std::ffi::{c_char, c_int, CStr, CString};

use pg_query::{protobuf, ParseResult, ParserOptions};
use prost::Message;

use crate::sqlstate::SQLSTATE_STATEMENT_TOO_COMPLEX;
use crate::ParseError;

/// The deepest nesting of protobuf fields that the decode accepts. In a
/// release build on a 2 MiB stack, the default of a tokio worker, each
/// statement that was measured at this depth ran, and CTEs nested at this
/// depth inside a chain of CTEs overflowed the stack no earlier than the
/// chain alone does. Subqueries nested over a long chain of CTEs can overflow
/// a 2 MiB stack within the engine's limit on nested queries, whatever this
/// bound is.
const MAX_PARSE_TREE_DEPTH: usize = 450;

// The declarations below are copied from libpg_query's pg_query.h, in
// pg_query 18.1.0.

#[repr(C)]
struct PgQueryError {
    // The fields after `message` are not read.
    message: *mut c_char,
}

#[repr(C)]
struct PgQueryProtobuf {
    len: usize,
    data: *mut c_char,
}

#[repr(C)]
struct PgQueryProtobufParseResult {
    parse_tree: PgQueryProtobuf,
    stderr_buffer: *mut c_char,
    error: *mut PgQueryError,
}

extern "C" {
    fn pg_query_parse_protobuf_opts(
        input: *const c_char,
        parser_options: c_int,
    ) -> PgQueryProtobufParseResult;
    fn pg_query_free_protobuf_parse_result(result: PgQueryProtobufParseResult);
}

/// Parses `sql` as `pg_query::parse` does. A parse tree nested deeper than
/// [MAX_PARSE_TREE_DEPTH] is the outer error, which PostgreSQL gives for a
/// statement that does not fit its stack. The inner result is what
/// `pg_query::parse` returns.
pub(crate) fn parse_bounded(sql: &str) -> Result<pg_query::Result<ParseResult>, ParseError> {
    let input = match CString::new(sql) {
        Ok(input) => input,
        Err(error) => return Ok(Err(error.into())),
    };
    // SAFETY: `input` is a NUL-terminated string that outlives the call.
    let result =
        unsafe { pg_query_parse_protobuf_opts(input.as_ptr(), ParserOptions::DEFAULT.bits()) };
    let parsed = read_parse_result(&result);
    // SAFETY: `result` comes from `pg_query_parse_protobuf_opts` and is freed
    // once. `parsed` owns copies of everything it needs from it.
    unsafe { pg_query_free_protobuf_parse_result(result) };
    parsed
}

fn read_parse_result(
    result: &PgQueryProtobufParseResult,
) -> Result<pg_query::Result<ParseResult>, ParseError> {
    if !result.error.is_null() {
        // SAFETY: a non-null `error` points to an error whose `message` is a
        // NUL-terminated string, valid until the result is freed.
        let message = unsafe { CStr::from_ptr((*result.error).message) };
        let message = message.to_string_lossy().into_owned();
        return Ok(Err(pg_query::Error::Parse(message)));
    }
    let tree = &result.parse_tree;
    // SAFETY: without an error, `data` points to `len` bytes of the encoded
    // tree, valid until the result is freed. `len` is never 0, because the
    // tree always holds the PostgreSQL version.
    let bytes = unsafe { std::slice::from_raw_parts(tree.data.cast::<u8>(), tree.len) };
    if nests_deeper_than(bytes, MAX_PARSE_TREE_DEPTH) {
        return Err(ParseError::Sqlstate {
            sqlstate: SQLSTATE_STATEMENT_TOO_COMPLEX,
            message: "stack depth limit exceeded".to_string(),
        });
    }
    // SAFETY: without an error, `stderr_buffer` is a NUL-terminated string,
    // valid until the result is freed.
    let stderr = unsafe { CStr::from_ptr(result.stderr_buffer) };
    let stderr = stderr.to_string_lossy().into_owned();
    let decoded = protobuf::ParseResult::decode(bytes).map_err(pg_query::Error::Decode);
    Ok(decoded.map(|tree| ParseResult::new(tree, stderr)))
}

/// Whether the length-delimited fields of the protobuf message `bytes` nest
/// more than `limit` deep. Each nested message is such a field, so the depth
/// is at least the depth of the messages. A string or bytes field counts as
/// a level too, and so do the fields in it that its bytes happen to encode,
/// so the count can be larger, never smaller. The loop reads each byte once
/// and does not recurse.
fn nests_deeper_than(bytes: &[u8], limit: usize) -> bool {
    // The end offsets of the length-delimited fields around `pos`, the
    // innermost last.
    let mut ends: Vec<usize> = Vec::new();
    let mut pos = 0;
    loop {
        while ends.last() == Some(&pos) {
            ends.pop();
        }
        let end = ends.last().copied().unwrap_or(bytes.len());
        if pos == end {
            return false;
        }
        match read_field(&bytes[..end], pos) {
            Some(Field::Scalar { next }) => pos = next,
            Some(Field::LengthDelimited { start, end }) => {
                ends.push(end);
                if ends.len() > limit {
                    return true;
                }
                pos = start;
            }
            // The bytes of a string, of bytes or of packed numbers need not
            // encode fields, and such a field holds no message.
            None => pos = end,
        }
    }
}

enum Field {
    /// A number, which ends at `next`.
    Scalar { next: usize },
    /// A message, string, bytes or packed numbers in `start..end`.
    LengthDelimited { start: usize, end: usize },
}

/// The field that starts at `pos` of `bytes`, or `None` if the bytes there
/// do not encode a field that ends within `bytes`.
fn read_field(bytes: &[u8], pos: usize) -> Option<Field> {
    let (key, pos) = read_varint(bytes, pos)?;
    let next = match key & 7 {
        0 => read_varint(bytes, pos)?.1,
        1 => pos + 8,
        2 => {
            let (len, start) = read_varint(bytes, pos)?;
            let end = start.checked_add(usize::try_from(len).ok()?)?;
            return (end <= bytes.len()).then_some(Field::LengthDelimited { start, end });
        }
        5 => pos + 4,
        // Groups (3 and 4) are not in pg_query.proto, and 6 and 7 are not
        // wire types.
        _ => return None,
    };
    (next <= bytes.len()).then_some(Field::Scalar { next })
}

/// The varint at `pos` of `bytes` and the offset after it.
fn read_varint(bytes: &[u8], mut pos: usize) -> Option<(u64, usize)> {
    let mut value = 0;
    for shift in (0..64).step_by(7) {
        let byte = *bytes.get(pos)?;
        pos += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte < 0x80 {
            return Some((value, pos));
        }
    }
    None
}
