use std::sync::Arc;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use crate::{AtomicRegister, build_atomic_register, ClientRegisterCommand, RegisterClient, SectorIdx, SectorsManager, SuccessCallbackType, SystemRegisterCommand};
use crate::tcp_writer::TcpWriter;
use crate::transfer::Acknowledgment;

#[derive(Clone)]
pub struct RegisterQueue {
    client_tx: UnboundedSender<(ClientRegisterCommand, SuccessCallbackType)>,
    system_tx: UnboundedSender<(SystemRegisterCommand, Option<TcpWriter>)>
}

impl RegisterQueue {
    pub async fn build(sector_idx: SectorIdx,
                       rank: u8,
                       num_processes: u8,
                       register_client: Arc<dyn RegisterClient>,
                       sectors_manager: Arc<dyn SectorsManager>) -> Self {
        let (client_tx, client_rx) = unbounded_channel();
        let (system_tx, system_rx) = unbounded_channel();

        let register = build_atomic_register(
            rank, sector_idx, register_client, sectors_manager, num_processes
        ).await;

        let mut register_queue_background = RegisterQueueBackground::build(
            client_rx, system_rx, register, rank
        ).await;

        tokio::spawn(async move { register_queue_background.run().await; });

        Self {
            client_tx,
            system_tx
        }
    }

    pub fn add_client_cmd(&self, cmd: ClientRegisterCommand, cb: SuccessCallbackType) {
        _ = self.client_tx.send((cmd, cb));
    }

    pub fn add_system_cmd(&self, cmd: SystemRegisterCommand, writer: Option<TcpWriter>) {
        _ = self.system_tx.send((cmd, writer));
    }
}

struct RegisterQueueBackground {
    client_rx: UnboundedReceiver<(ClientRegisterCommand, SuccessCallbackType)>,
    system_rx: UnboundedReceiver<(SystemRegisterCommand, Option<TcpWriter>)>,
    client_return_rx: UnboundedReceiver<()>,
    register: Box<dyn AtomicRegister>,
    rank: u8,
    is_client_cmd_executing: bool,

    client_return_tx: UnboundedSender<()>,
}

impl RegisterQueueBackground {
    async fn build(client_rx: UnboundedReceiver<(ClientRegisterCommand, SuccessCallbackType)>,
                 system_rx: UnboundedReceiver<(SystemRegisterCommand, Option<TcpWriter>)>,
                 register: Box<dyn AtomicRegister>,
                 rank: u8) -> Self {
        let (client_return_tx, client_return_rx) = unbounded_channel();

        RegisterQueueBackground {
            client_rx,
            system_rx,
            client_return_rx,
            register,
            rank,
            is_client_cmd_executing: false,
            client_return_tx,
        }
    }

    async fn run(&mut self) {
        let mut keep_going = true;

        while keep_going {
            keep_going = if self.is_client_cmd_executing {
                self.wait_cmd_occupied_client().await
            } else {
                self.wait_cmd_free_client().await
            };
        }
    }

    async fn wait_cmd_free_client(&mut self) -> bool {
        tokio::select! {
            biased;
            Some((cmd, cb)) = self.client_rx.recv() => {
                let client_return_tx = self.client_return_tx.clone();
                self.register.client_command(cmd, Box::new(|success| Box::pin(
                    async move { _ = client_return_tx.send(()); cb(success).await; })
                )).await;
                self.is_client_cmd_executing = true;
            },
            Some((cmd, writer)) = self.system_rx.recv() => {
                self.register.system_command(cmd.clone()).await;
                if let Some(writer) = writer { writer.send_ack(Acknowledgment::from_cmd(cmd, self.rank)); }
            },
            else => return false
        }

        true
    }

    async fn wait_cmd_occupied_client(&mut self) -> bool {
        tokio::select! {
            biased;
            Some(()) = self.client_return_rx.recv() => self.is_client_cmd_executing = false,
            Some((cmd, writer)) = self.system_rx.recv() => {
                self.register.system_command(cmd.clone()).await;
                if let Some(writer) = writer { writer.send_ack(Acknowledgment::from_cmd(cmd, self.rank)); }
            },
            else => return false
        }

        true
    }
}