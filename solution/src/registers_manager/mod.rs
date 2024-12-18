use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::{AtomicRegister, build_atomic_register, RegisterClient, SectorIdx, SectorsManager};

pub struct RegistersManager {
    active_registers: HashMap<SectorIdx, (Arc<Mutex<Box<dyn AtomicRegister>>>, u64)>,

    rank: u8,
    register_client: Arc<dyn RegisterClient>,
    sectors_manager: Arc<dyn SectorsManager>,
    processes_count: u8
}

impl RegistersManager {
    pub fn build(rank: u8,
                 register_client: Arc<dyn RegisterClient>,
                 sectors_manager: Arc<dyn SectorsManager>,
                 processes_count: u8) -> Self {
        Self {
            active_registers: HashMap::new(),
            rank,
            register_client,
            sectors_manager,
            processes_count
        }
    }

    pub async fn get(&mut self, sector_idx: &SectorIdx) -> Arc<Mutex<Box<dyn AtomicRegister>>> {
        if let Some(entry) = self.active_registers.get_mut(sector_idx) {
            entry.1 += 1;
            entry.0.clone()
        } else {
            let register = Arc::new(Mutex::new(build_atomic_register(
                self.rank, sector_idx.clone(), self.register_client.clone(), self.sectors_manager.clone(), self.processes_count
            ).await));

            self.active_registers.insert(sector_idx.clone(), (register.clone(), 1));
            register
        }
    }

    pub fn remove(&mut self, sector_idx: &SectorIdx) {
        if let Some(entry) = self.active_registers.get_mut(sector_idx) {
            if entry.1 == 1 {
                self.active_registers.remove(sector_idx);
            } else {
                entry.1 -= 1;
            }
        }
    }
}