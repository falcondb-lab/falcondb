fn main() -> Result<(), Box<dyn std::error::Error>> {
    let replication_proto = "proto/falcon_replication.proto";
    let raft_proto = "proto/falcon_raft.proto";
    println!("cargo:rerun-if-changed={replication_proto}");
    println!("cargo:rerun-if-changed={raft_proto}");
    println!("cargo:rerun-if-changed=proto");

    // Use the vendored protoc binary so the build works without a system install.
    let protoc = protoc_bin_vendored::protoc_bin_path()
        .expect("protoc-bin-vendored: could not locate vendored protoc binary");
    std::env::set_var("PROTOC", protoc);

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&[replication_proto, raft_proto], &["proto"])?;

    Ok(())
}
