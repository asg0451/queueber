use color_eyre::Result;

fn main() -> Result<()> {
    queueber::protocol::demo()?;
    Ok(())
}
