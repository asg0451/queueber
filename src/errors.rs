use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("capnp error: {0}")]
    Capnp(#[from] capnp::Error),
    #[error("other error: {0}")]
    Other(#[from] Box<dyn std::error::Error + Send + Sync + 'static>),
}

pub type Result<T> = std::result::Result<T, Error>;
