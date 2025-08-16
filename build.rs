fn main() {
    ::capnpc::CompilerCommand::new()
        .file("queueber.capnp")
        .run()
        .expect("compiling schema");
}
