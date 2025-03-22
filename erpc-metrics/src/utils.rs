use tokio::io::AsyncWriteExt;

/// Decompress given compressed slice of xz bytes
pub(crate) async fn decompress_xz(data: &[u8]) -> anyhow::Result<Vec<u8>> {
    let mut decoder = async_compression::tokio::write::XzDecoder::new(Vec::new());
    decoder.write_all(data).await?;
    decoder.shutdown().await?;
    Ok(decoder.into_inner())
}
