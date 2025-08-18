use std::backtrace::Backtrace;
use thiserror::Error;
use tokio::task::JoinError;

#[derive(Error, Debug)]
pub enum Error {
    #[error("assertion failed: {msg}")]
    AssertionFailed { msg: String, backtrace: Backtrace },

    #[error("io error: {source}")]
    Io {
        #[from]
        source: std::io::Error,
        backtrace: Backtrace,
    },
    #[error("capnp error: {source}")]
    Capnp {
        #[from]
        source: capnp::Error,
        backtrace: Backtrace,
    },
    #[error("other error: {source}")]
    Other {
        #[from]
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
        backtrace: Backtrace,
    },
    #[error("rocksdb error: {source}")]
    Rocksdb {
        #[from]
        source: rocksdb::Error,
        backtrace: Backtrace,
    },
    #[error("join error: {source}")]
    Join {
        #[from]
        source: JoinError,
        backtrace: Backtrace,
    },
    #[error("system time error: {source}")]
    SystemTime {
        #[from]
        source: std::time::SystemTimeError,
        backtrace: Backtrace,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<Error> for capnp::Error {
    fn from(err: Error) -> Self {
        // Convert any internal error to a failed Cap'n Proto RPC error.
        capnp::Error::failed(err.to_string())
    }
}

