extern crate protoc_rust;

fn main() {
    let files = &[
        "protobuf/acl.proto",
        "protobuf/ClientNamenodeProtocol.proto",
        "protobuf/datatransfer.proto",
        "protobuf/erasurecoding.proto",
        "protobuf/encryption.proto",
        "protobuf/HAServiceProtocol.proto",
        "protobuf/hdfs.proto",
        "protobuf/inotify.proto",
        "protobuf/IpcConnectionContext.proto",
        "protobuf/ProtobufRpcEngine.proto",
        "protobuf/RpcHeader.proto",
        "protobuf/Security.proto",
        "protobuf/xattr.proto",
    ];
    for file in files {
        println!("cargo:rerun-if-changed={}", file);
    }
    protoc_rust::Codegen::new()
        .out_dir("src")
        .inputs(files)
        .include("protobuf")
        .run()
        .expect("protoc");
}
