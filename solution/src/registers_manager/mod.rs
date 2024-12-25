use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use crate::{ClientRegisterCommand, RegisterClient, SectorIdx, SectorsManager, SuccessCallbackType, SystemCallbackType, SystemRegisterCommand};
use crate::registers_manager::register_handler::RegisterHandler;

mod register_handler;

#[derive(Clone)]
pub struct RegistersManager {
    client_tx: UnboundedSender<(ClientRegisterCommand, SuccessCallbackType)>,
    system_tx: UnboundedSender<(SystemRegisterCommand, SystemCallbackType)>
}

impl RegistersManager {
    pub fn build(rank: u8,
                 register_client: Arc<dyn RegisterClient>,
                 sectors_manager: Arc<dyn SectorsManager>,
                 processes_count: u8) -> Self {
        let (system_tx, system_rx) = unbounded_channel();
        let (client_tx, client_rx) = unbounded_channel();

        tokio::spawn(Self::background(
           rank, register_client, sectors_manager, processes_count, system_rx, client_rx
        ));

        Self {
            client_tx,
            system_tx
        }
    }

    async fn background(rank: u8,
                        register_client: Arc<dyn RegisterClient>,
                        sectors_manager: Arc<dyn SectorsManager>,
                        processes_count: u8,
                        mut system_rx: UnboundedReceiver<(SystemRegisterCommand, SystemCallbackType)>,
                        mut client_rx: UnboundedReceiver<(ClientRegisterCommand, SuccessCallbackType)>) {
        let mut active_registers: HashMap<SectorIdx, RegisterHandler> = HashMap::new();

        loop {
            tokio::select! {
                Some((cmd, cb)) = system_rx.recv() => {
                    let idx = cmd.header.sector_idx;

                    let handler = Self::get_from_idx(
                        idx,
                        &mut active_registers,
                        rank,
                        register_client.clone(),
                        sectors_manager.clone(),
                        processes_count
                    ).await;

                    handler.enqueue_system_cmd(cmd, cb);
                },
                Some((cmd, cb)) = client_rx.recv() => {
                    let idx = cmd.header.sector_idx;

                    let handler = Self::get_from_idx(
                        idx,
                        &mut active_registers,
                        rank,
                        register_client.clone(),
                        sectors_manager.clone(),
                        processes_count
                    ).await;

                    handler.enqueue_client_cmd(cmd, cb);
                },
                else => break
            }
        }
    }

    async fn get_from_idx(idx: SectorIdx,
                    active_registers: &mut HashMap<SectorIdx, RegisterHandler>,
                    rank: u8,
                    register_client: Arc<dyn RegisterClient>,
                    sectors_manager: Arc<dyn SectorsManager>,
                    processes_count: u8) -> RegisterHandler {
        let opt = active_registers.get(&idx);

        match opt {
            None => {
                let handler = RegisterHandler::build(
                    idx, rank, processes_count, register_client, sectors_manager
                ).await;

                active_registers.insert(idx, handler.clone());
                handler
            }
            Some(handler) => handler.clone()
        }
    }

    pub fn add_client_cmd(&self, cmd: ClientRegisterCommand, cb: SuccessCallbackType) {
        let _ = self.client_tx.send((cmd, cb));
    }

    pub fn add_system_cmd(&self, cmd: SystemRegisterCommand, cb: SystemCallbackType) {
        let _ = self.system_tx.send((cmd, cb));
    }
}