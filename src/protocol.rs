capnp::generated_code!(pub mod queueber_capnp);
pub use queueber_capnp::*;

use crate::errors::Result;
use capnp::serialize as serialize_mode;
use capnp::{
    message::{ReaderOptions, TypedReader},
    serialize::OwnedSegments,
};

pub fn demo() -> Result<()> {
    // create & write
    let mut message = ::capnp::message::Builder::new_default();
    let mut item = message.init_root::<item::Builder>();
    item.set_contents(b"hello");
    item.set_visibility_timeout_secs(10);

    serialize_mode::write_message(&mut ::std::io::stdout(), &message)?;

    let mut buf = Vec::new();
    serialize_mode::write_message(&mut buf, &message)?;

    // read
    let message_reader =
        serialize_mode::read_message(&buf[..], ::capnp::message::ReaderOptions::new())?;

    let item = message_reader.get_root::<item::Reader>()?;

    println!("{:?}", item.get_contents()?);

    Ok(())
}

// call t.get()? to get the item::Reader
pub fn item_from_bytes(bytes: &[u8]) -> Result<TypedReader<OwnedSegments, item::Owned>> {
    let message = serialize_mode::read_message(bytes, ReaderOptions::new())?;
    let t: TypedReader<OwnedSegments, item::Owned> = TypedReader::new(message);
    Ok(t)
}
