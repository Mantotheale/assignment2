use crate::{SectorIdx, SectorVec};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::File;
use crate::sectors_manager::{read_hash_content, remove_file, StableSectorsManager, write_file};

#[async_trait::async_trait]
pub trait SectorsManager: Send + Sync {
    /// Returns 4096 bytes of sector data by index.
    async fn read_data(&self, idx: SectorIdx) -> SectorVec;

    /// Returns timestamp and write rank of the process which has saved this data.
    /// Timestamps and ranks are relevant for atomic register algorithm, and are described
    /// there.
    async fn read_metadata(&self, idx: SectorIdx) -> (u64, u8);

    /// Writes a new data, along with timestamp and write rank to some sector.
    async fn write(&self, idx: SectorIdx, sector: &(SectorVec, u64, u8));
}

/// Path parameter points to a directory to which this method has exclusive access.
pub async fn build_sectors_manager(path: PathBuf) -> Arc<dyn SectorsManager> {
    let mut paths = tokio::fs::read_dir(path.clone()).await.unwrap();

    while let Ok(Some(entry)) = paths.next_entry().await {
        let entry_name = entry.file_name();
        let file_name = entry_name.to_str().unwrap();

        if file_name.starts_with("tmp_") {
            let tmp_path = entry.path();
            let dir = File::open(path.clone()).await.unwrap();

            if let Ok(content) = read_hash_content(&tmp_path).await {
                let dst_path = path.join(&file_name[4..]);
                write_file(&dst_path, &dir, &content).await;
            }

            remove_file(&tmp_path, &dir).await;
        }
    }

    Arc::new(StableSectorsManager {
        dir: path
    })
}