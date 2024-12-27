use std::io::SeekFrom;
use std::path::PathBuf;
use std::sync::Arc;
use sha2::Digest;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::{Semaphore, SemaphorePermit};
use crate::{SectorIdx, SectorVec};

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
    Arc::new(StableSectorsManager::build(path).await)
}

const SECTOR_SIZE: usize = 4096;
const MAX_OPEN_FILE_DESCRIPTORS: usize = 1024;

struct StableSectorsManager {
    dir: PathBuf,
    semaphore: Semaphore
}

#[async_trait::async_trait]
impl SectorsManager for StableSectorsManager {
    async fn read_data(&self, idx: SectorIdx) -> SectorVec {
        let file_path = self.dir.join(idx.to_string());

        let mut vec_buf = [0u8; SECTOR_SIZE];

        if file_path.exists() {
            let _permit = self.gain_access().await;
            let mut file = File::open(file_path).await.unwrap();

            file.read_exact(&mut vec_buf).await.unwrap();
        }

        SectorVec {
            0: vec_buf.to_vec(),
        }
    }

    async fn read_metadata(&self, idx: SectorIdx) -> (u64, u8) {
        let file_path = self.dir.join(idx.to_string());

        let mut timestamp = 0u64;
        let mut write_rank = 0u8;

        if file_path.exists() {
            let _permit = self.gain_access().await;
            let mut file = File::open(file_path).await.unwrap();
            file.seek(SeekFrom::Start(SECTOR_SIZE as u64)).await.unwrap();
            timestamp = file.read_u64().await.unwrap();
            write_rank = file.read_u8().await.unwrap();
        }

        (timestamp, write_rank)
    }

    async fn write(&self, idx: SectorIdx, sector: &(SectorVec, u64, u8)) {
        let dest_path = self.dir.join(idx.to_string());
        let tmp_path = self.dir.join(format!("tmp_{}", idx.to_string()));

        let content = [
            sector.0.0.as_slice(),
            &sector.1.to_be_bytes(),
            &sector.2.to_be_bytes()
        ].concat();

        let tmp_content = [
            sha2::Sha256::digest(&content).as_slice(),
            content.as_slice()
        ].concat();

        let _permit = self.gain_access().await;
        let dir = File::open(&self.dir).await.unwrap();

        self.write_file(&tmp_path, &dir, tmp_content.as_slice()).await;
        self.write_file(&dest_path, &dir, content.as_slice()).await;
        self.remove_file(&tmp_path, &dir).await;
    }
}

impl StableSectorsManager {
    async fn build(path: PathBuf) -> Self {
        let sectors_manager = Self {
            dir: path.clone(),
            semaphore: Semaphore::new(MAX_OPEN_FILE_DESCRIPTORS)
        };

        let mut paths = tokio::fs::read_dir(path.clone()).await.unwrap();

        while let Ok(Some(entry)) = paths.next_entry().await {
            let entry_name = entry.file_name();
            let file_name = entry_name.to_str().unwrap();

            if file_name.starts_with("tmp_") {
                let tmp_path = entry.path();

                let _permit1 = sectors_manager.gain_access().await;
                let dir = File::open(path.clone()).await.unwrap();

                if let Ok(content) = sectors_manager.read_hash_content(&tmp_path).await {
                    let dst_path = path.join(&file_name[4..]);
                    sectors_manager.write_file(&dst_path, &dir, &content).await;
                }

                sectors_manager.remove_file(&tmp_path, &dir).await;
            }
        }

        sectors_manager
    }

    async fn gain_access(&self) -> SemaphorePermit {
        self.semaphore.acquire().await.unwrap()
    }

    async fn read_hash_content(&self, file_path: &PathBuf) -> Result<[u8; SECTOR_SIZE + 8 + 1], ()> {
        let _permit = self.gain_access().await;
        let mut file = File::open(file_path).await.unwrap();

        let mut hash = [0u8; 32];
        file.read_exact(&mut hash).await.map_err(|_|())?;

        let mut content = [0u8; SECTOR_SIZE + 8 + 1];
        file.read_exact(&mut content).await.map_err(|_|())?;

        if hash == sha2::Sha256::digest(&content).as_slice() {
            Ok(content)
        } else {
            Err(())
        }
    }

    async fn write_file(&self, file_path: &PathBuf, parent_dir: &File, content: &[u8]) {
        let _permit = self.gain_access().await;
        let mut file = File::create(file_path).await.unwrap();
        file.write_all(content).await.unwrap();

        file.sync_data().await.unwrap();
        parent_dir.sync_data().await.unwrap();
    }

    async fn remove_file(&self, file_path: &PathBuf, parent_dir: &File) {
        tokio::fs::remove_file(file_path).await.unwrap();
        parent_dir.sync_data().await.unwrap();
    }
}