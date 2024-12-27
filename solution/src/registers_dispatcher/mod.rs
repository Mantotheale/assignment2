use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use crate::{ClientRegisterCommand, RegisterClient, SectorIdx, SectorsManager, SuccessCallbackType, SystemRegisterCommand};
use crate::registers_dispatcher::register_queue::RegisterQueue;
use crate::tcp_writer::TcpWriter;

mod register_queue;

#[derive(Clone)]
pub struct RegistersDispatcher {
    client_tx: UnboundedSender<(ClientRegisterCommand, SuccessCallbackType)>,
    system_tx: UnboundedSender<(SystemRegisterCommand, Option<TcpWriter>)>
}

impl RegistersDispatcher {
    pub fn build(system_tx: UnboundedSender<(SystemRegisterCommand, Option<TcpWriter>)>,
                 system_rx: UnboundedReceiver<(SystemRegisterCommand, Option<TcpWriter>)>,
                 rank: u8,
                 register_client: Arc<dyn RegisterClient>,
                 sectors_manager: Arc<dyn SectorsManager>,
                 num_processes: u8) -> Self {
        let (client_tx, client_rx) = unbounded_channel();

        let mut dispatcher_background = DispatcherBackground::build(
            client_rx, system_rx, rank, register_client, sectors_manager, num_processes
        );

        tokio::spawn(async move { dispatcher_background.run().await; });

        Self { client_tx, system_tx }
    }
    pub fn add_client_cmd(&self, cmd: ClientRegisterCommand, cb: SuccessCallbackType) {
        let _ = self.client_tx.send((cmd, cb));
    }

    pub fn add_system_cmd(&self, cmd: SystemRegisterCommand, writer: Option<TcpWriter>) {
        let _ = self.system_tx.send((cmd, writer));
    }
}

struct DispatcherBackground {
    client_rx: UnboundedReceiver<(ClientRegisterCommand, SuccessCallbackType)>,
    system_rx: UnboundedReceiver<(SystemRegisterCommand, Option<TcpWriter>)>,
    active_registers: HashMap<SectorIdx, RegisterQueue>,

    rank: u8,
    register_client: Arc<dyn RegisterClient>,
    sectors_manager: Arc<dyn SectorsManager>,
    processes_count: u8
}

impl DispatcherBackground {
    fn build(client_rx: UnboundedReceiver<(ClientRegisterCommand, SuccessCallbackType)>,
             system_rx: UnboundedReceiver<(SystemRegisterCommand, Option<TcpWriter>)>,
             rank: u8,
             register_client: Arc<dyn RegisterClient>,
             sectors_manager: Arc<dyn SectorsManager>,
             processes_count: u8) -> Self {
        Self { client_rx, system_rx, rank, register_client, sectors_manager, processes_count, active_registers: HashMap::new() }
    }

    async fn run(&mut self) {
        loop {
            tokio::select! {
                Some((cmd, cb)) = self.client_rx.recv() => {
                    let idx = cmd.header.sector_idx;
                    let queue = self.get_from_idx(idx).await;
                    queue.add_client_cmd(cmd, cb);
                },
                Some((cmd, writer)) = self.system_rx.recv() => {
                    let idx = cmd.header.sector_idx;
                    let queue = self.get_from_idx(idx).await;
                    queue.add_system_cmd(cmd, writer);
                },
                else => break
            }
        }
    }

    async fn get_from_idx(&mut self, idx: SectorIdx) -> RegisterQueue {
        if let Some(queue) = self.active_registers.get(&idx) {
            queue.clone()
        } else {
            let queue = RegisterQueue::build(
                idx, self.rank, self.processes_count, self.register_client.clone(), self.sectors_manager.clone()
            ).await;

            self.active_registers.insert(idx, queue.clone());
            queue
        }
    }
}