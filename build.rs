// NOTE: This build script requires the Cap'n Proto CLI (`capnp`) to be installed
// and available on PATH. If you see a failure here, install it from
// https://capnproto.org/install.html (version 0.5.2+).
fn main() {
    if let Err(err) = ::capnpc::CompilerCommand::new()
        .file("queueber.capnp")
        .run()
    {
        eprintln!(
            "Cap'n Proto schema compilation failed.\n\n  Hint: Install the 'capnp' CLI (>= 0.5.2) and ensure it is on PATH.\n  See: https://capnproto.org/install.html\n\nError: {err}"
        );
        std::process::exit(1);
    }
}
