mod acl;
mod auth_functions;
mod auth_members;
mod catalog;
mod classes;
mod copy;
mod deparse;
mod descriptions;
mod error;
mod functions;
mod open;
mod options;
mod pg_type;
mod policies;
mod roles;
mod schema_init;
mod schema_state;
mod session;
mod settings;
mod statement;
mod text;
mod unique_index;
mod unnest;

pub use catalog::PostgresDialect;
pub use copy::CopyTextEncoder;
pub use error::{sql_parse_error, sqlstate_error, Error, ErrorReport, Result};
pub use open::{open_database_async, OpenDatabaseState};
pub use session::PgConnection as Connection;
pub use session::{
    open_database, open_database_with_io, split_statements, tursopg_database_opts, PgConnection,
    PgQueryRunner, TransactionState,
};
pub use statement::{Statement, StatementResultKind};
pub use text::{value_text, TextType};
pub use turso_core::{
    types, Database, DatabaseOpts, Func, LimboError, Numeric, OpenFlags, PlatformIO, StepResult,
};

pub mod vtab {
    pub use turso_core::VirtualTable;
}

/// The stack of each thread that runs statements in a debug build of the
/// tursopg server, its CLI and the sqltest runner. The stack frames of a debug
/// build are many times larger than those of a release build, and statements
/// well within the depth limits overflow the default stack of a tokio worker
/// or of the main thread there. A release build keeps the default stacks.
#[doc(hidden)]
pub const DEBUG_THREAD_STACK_SIZE: usize = 32 << 20;
