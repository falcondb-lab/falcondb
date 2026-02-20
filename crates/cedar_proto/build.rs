fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_file = "proto/cedar_replication.proto";
    println!("cargo:rerun-if-changed={}", proto_file);
    println!("cargo:rerun-if-changed=proto");

    // Use the vendored protoc binary so the build works without a system install.
    let protoc = protoc_bin_vendored::protoc_bin_path()
        .expect("protoc-bin-vendored: could not locate vendored protoc binary");
    std::env::set_var("PROTOC", protoc);

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&[proto_file], &["proto"])?;

    Ok(())
}
