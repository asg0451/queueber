capnp::generated_code!(pub mod queueber_capnp);

use color_eyre::Result;

fn main() -> Result<()> {
    use crate::queueber_capnp::item;
    use capnp::serialize_packed;

    // create & write
    let mut message = ::capnp::message::Builder::new_default();
    let mut item = message.init_root::<item::Builder>();
    item.set_contents(b"hello");
    item.set_visibility_timeout_secs(10);
    item.set_id(42);

    serialize_packed::write_message(&mut ::std::io::stdout(), &message)?;

    let mut buf = Vec::new();
    serialize_packed::write_message(&mut buf, &message)?;

    // read
    let message_reader =
        serialize_packed::read_message(&buf[..], ::capnp::message::ReaderOptions::new())?;

    let item = message_reader.get_root::<item::Reader>()?;

    println!("{:?}", item.get_contents()?);

    Ok(())
}
