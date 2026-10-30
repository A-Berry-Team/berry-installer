//! Sophon manifest wire format (protobuf), written out by hand so the crate needs no `protoc`
//! and no build script. Field numbers match HoYoverse's published schema.

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SophonManifest {
    #[prost(message, repeated, tag = "1")]
    pub files: Vec<ManifestFile>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ManifestFile {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(message, repeated, tag = "2")]
    pub chunks: Vec<FileChunk>,
    /// `64` marks a directory entry.
    #[prost(int32, tag = "3")]
    pub r#type: i32,
    #[prost(uint64, tag = "4")]
    pub size: u64,
    #[prost(string, tag = "5")]
    pub md5: String,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FileChunk {
    /// Name on the CDN (`<chunk_base>/<chunk_name>`).
    #[prost(string, tag = "1")]
    pub chunk_name: String,
    /// MD5 of the chunk *after* zstd decompression: what the file region must hash to.
    #[prost(string, tag = "2")]
    pub chunk_decompressed_md5: String,
    /// Where the decompressed chunk goes inside the file.
    #[prost(uint64, tag = "3")]
    pub chunk_on_file_offset: u64,
    /// Compressed size: the bytes on the wire.
    #[prost(uint64, tag = "4")]
    pub chunk_size: u64,
    #[prost(uint64, tag = "5")]
    pub chunk_decompressed_size: u64,
    #[prost(uint64, tag = "6")]
    pub something: u64,
    /// MD5 of the compressed bytes as served.
    #[prost(string, tag = "7")]
    pub chunk_md5: String,
}

pub const TYPE_DIRECTORY: i32 = 64;
