//! Deeply nested statements through the PostgreSQL frontend: they run as in
//! PostgreSQL up to tursopg's limits, and past them they fail with SQLSTATE
//! 54001, which PostgreSQL gives for a statement that does not fit its stack.

use crate::common::TempDatabase;
use turso_core::ast::MAX_QUERY_DEPTH;

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

#[test]
fn query_nesting_past_the_engine_limit_is_too_complex() {
    assert_eq!(first_value(cte_chain(MAX_QUERY_DEPTH)), Ok("1".to_string()));
    assert_eq!(
        first_value(cte_chain(MAX_QUERY_DEPTH + 1)),
        Err("54001: Query is nested too deeply (maximum depth 500)".to_string())
    );
}

#[test]
fn libpg_query_stack_depth_error_is_too_complex() {
    let sql = format!("SELECT 1{}", "+1".repeat(100_000));
    assert_eq!(
        first_value(sql),
        Err("54001: stack depth limit exceeded".to_string())
    );
}

#[test]
fn view_whose_stored_text_does_not_parse_is_too_complex() {
    // core stores a view as SQLite text, where each `+` adds two levels of
    // expression depth, and the SQLite parser allows 100.
    let fits = format!("CREATE VIEW fits AS SELECT 1{} AS x", "+1".repeat(50));
    let too_deep = format!("CREATE VIEW too_deep AS SELECT 1{} AS x", "+1".repeat(51));
    let created = on_worker_stack(move || {
        let db = TempDatabase::builder().build();
        let conn = db.connect_postgres();
        conn.execute(&fits).unwrap();
        let refused = conn.execute(&too_deep).map_err(error_text);
        drop(conn);
        (db.reopen(), refused)
    });
    let (db, refused) = created;
    assert_eq!(
        refused,
        Err("54001: view \"too_deep\" cannot be stored: \
             Expression tree is too large (maximum depth 100)"
            .to_string())
    );
    let value = on_worker_stack(move || value_in(&db, "SELECT x FROM fits"));
    assert_eq!(value, Ok("51".to_string()));
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

/// A `WITH` clause of `length` CTEs, each of which reads the one before it.
/// The SQL text is flat, but the plan nests the CTEs `length` deep.
fn cte_chain(length: usize) -> String {
    let mut ctes = vec!["n0 AS (SELECT 1 AS a)".to_string()];
    ctes.extend((1..length).map(|i| format!("n{i} AS (SELECT a FROM n{})", i - 1)));
    format!("WITH {} SELECT a FROM n{}", ctes.join(", "), length - 1)
}

/// Runs `sql` in a new database on a thread with [WORKER_STACK] bytes of
/// stack. Returns the first value of the first row, or the SQLSTATE and the
/// message of the error.
fn first_value(sql: String) -> Result<String, String> {
    on_worker_stack(move || value_in(&TempDatabase::builder().build(), &sql))
}

/// The first value of the first row of `sql` in `db`, or the SQLSTATE and
/// the message of the error.
fn value_in(db: &TempDatabase, sql: &str) -> Result<String, String> {
    let conn = db.connect_postgres();
    let mut statement = conn
        .query(sql)
        .map_err(error_text)?
        .expect("a SELECT returns rows");
    let rows = statement.run_collect_rows().map_err(error_text)?;
    Ok(rows[0][0].to_string())
}

fn error_text(error: turso_pg::Error) -> String {
    format!("{}: {error}", error.sqlstate)
}

/// Runs `f` on a thread with [WORKER_STACK] bytes of stack.
fn on_worker_stack<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
    std::thread::Builder::new()
        .stack_size(WORKER_STACK)
        .spawn(f)
        .expect("spawn the thread of the statement")
        .join()
        .expect("the thread of the statement does not panic")
}
