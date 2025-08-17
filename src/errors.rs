use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("capnp error: {0}")]
    Capnp(#[from] capnp::Error),
    #[error("other error: {0}")]
    Other(#[from] Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("rocksdb error: {0}")]
    Rocksdb(#[from] rocksdb::Error),
    #[error("assertion failed: {0}")]
    AssertionFailed(String),
    #[error("system time error: {0}")]
    SystemTime(#[from] std::time::SystemTimeError),
}

pub type Result<T> = std::result::Result<T, Error>;
