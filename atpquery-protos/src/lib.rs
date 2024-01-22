static DESCRIPTOR_POOL: once_cell::sync::Lazy<prost_reflect::DescriptorPool> =
    once_cell::sync::Lazy::new(|| {
        prost_reflect::DescriptorPool::decode(include_bytes!("file_descriptor_set.bin").as_ref())
            .unwrap()
    });

include!(concat!(env!("OUT_DIR"), "/atpquery.rs"));
