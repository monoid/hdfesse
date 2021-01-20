extern crate protoc_rust;

fn main() {
    protoc_rust::Codegen::new()
        .out_dir("src/proto")
        .inputs(&[
            "protobuf/acl.proto",
            "protobuf/ClientNamenodeProtocol.proto",
            "protobuf/datatransfer.proto",
            "protobuf/HAServiceProtocol.proto",
            "protobuf/hdfs.proto",
            "protobuf/inotify.proto",
            "protobuf/IpcConnectionContext.proto",
            "protobuf/ProtobufRpcEngine.proto",
            "protobuf/RpcHeader.proto",
            "protobuf/Security.proto",
            "protobuf/xattr.proto",
        ])
        .include("protobuf")
        .run()
        .expect("protoc");
}
