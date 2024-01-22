fn main() {
    prost_reflect_build::Builder::new()
        .descriptor_pool("DESCRIPTOR_POOL")
        .compile_protos(&["src/row.proto", "src/checkpoint.proto"], &["src"])
        .unwrap();
}
