//! Deeply nested statements through the PostgreSQL frontend: they run as in
//! PostgreSQL up to tursopg's limits, and past them they fail with SQLSTATE
//! 54001, which PostgreSQL gives for a statement that does not fit its stack.

use crate::common::TempDatabase;

/// The stack of the thread that runs each statement, the same as in
/// `tests/integration/expr_depth_stack_overflow.rs`. The stack frames of a
/// debug build are many times larger than those of a release build.
const WORKER_STACK: usize = 64 << 20;

#[test]
fn nested_scalar_subqueries_run() {
    for depth in [16, 64] {
        let value = first_value(nested_scalar_subqueries(depth));
        assert_eq!(value, Ok("1".to_string()), "depth {depth}");
    }
}

#[test]
fn parse_tree_past_the_depth_bound_is_too_complex() {
    // The parse tree of 73 nested subqueries nests 446 protobuf fields deep,
    // and that of 74 nests 452. The bound is 450.
    assert_eq!(
        first_value(nested_scalar_subqueries(73)),
        Ok("1".to_string())
    );
    assert_eq!(
        first_value(nested_scalar_subqueries(74)),
        Err("54001: stack depth limit exceeded".to_string())
    );
}

#[test]
fn compound_select_runs() {
    for terms in [90, 400] {
        let value = first_value(compound_count(terms));
        assert_eq!(value, Ok(terms.to_string()), "{terms} terms");
    }
}

/// `SELECT (SELECT (... (SELECT 1)))` with `depth` nested subqueries.
fn nested_scalar_subqueries(depth: usize) -> String {
    format!("SELECT {}1{}", "(SELECT ".repeat(depth), ")".repeat(depth))
}

/// A count of the rows of a `UNION ALL` of `terms` terms.
fn compound_count(terms: usize) -> String {
    let compound = vec!["SELECT 1"; terms].join(" UNION ALL ");
    format!("SELECT count(*) FROM ({compound}) s")
}

/// Runs `sql` in a new database on a thread with [WORKER_STACK] bytes of
/// stack. Returns the first value of the first row, or the SQLSTATE and the
/// message of the error.
fn first_value(sql: String) -> Result<String, String> {
    std::thread::Builder::new()
        .stack_size(WORKER_STACK)
        .spawn(move || {
            let db = TempDatabase::builder().build();
            let conn = db.connect_postgres();
            let report = |error: turso_pg::Error| format!("{}: {error}", error.sqlstate);
            let mut statement = conn
                .query(&sql)
                .map_err(report)?
                .expect("a SELECT returns rows");
            let rows = statement.run_collect_rows().map_err(report)?;
            Ok(rows[0][0].to_string())
        })
        .expect("spawn the thread of the statement")
        .join()
        .expect("the thread of the statement does not panic")
}
