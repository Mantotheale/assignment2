use std::sync::Arc;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use crate::{AtomicRegister, build_atomic_register, ClientRegisterCommand, RegisterClient, SectorIdx, SectorsManager, SuccessCallbackType, SystemCallbackType, SystemRegisterCommand};

#[derive(Clone)]
pub struct RegisterHandler {
    client_tx: UnboundedSender<(ClientRegisterCommand, SuccessCallbackType)>,
    system_tx: UnboundedSender<(SystemRegisterCommand, SystemCallbackType)>
}

impl RegisterHandler {
    pub fn enqueue_client_cmd(&self, cmd: ClientRegisterCommand, cb: SuccessCallbackType) {
        let _ = self.client_tx.send((cmd, cb));
    }

    pub fn enqueue_system_cmd(&self, cmd: SystemRegisterCommand, cb: SystemCallbackType) {
        let _ = self.system_tx.send((cmd, cb));
    }
}

impl RegisterHandler {
    pub async fn build(sector_idx: SectorIdx,
                       rank: u8,
                       num_processes: u8,
                       register_client: Arc<dyn RegisterClient>,
                       sectors_manager: Arc<dyn SectorsManager>) -> Self {
        let (client_tx, client_rx) = unbounded_channel();
        let (system_tx, system_rx) = unbounded_channel();
        let (return_tx, return_rx) = unbounded_channel();

        let register = build_atomic_register(
            rank, sector_idx, register_client, sectors_manager, num_processes
        ).await;

        tokio::spawn(Self::handler_background(
            client_rx, system_rx, return_rx, return_tx, register
        ));

        Self {
            client_tx,
            system_tx
        }
    }

    async fn handler_background(mut client_rx: UnboundedReceiver<(ClientRegisterCommand, SuccessCallbackType)>,
                                mut system_rx: UnboundedReceiver<(SystemRegisterCommand, SystemCallbackType)>,
                                mut return_rx: UnboundedReceiver<()>,
                                return_tx: UnboundedSender<()>,
                                mut register: Box<dyn AtomicRegister>) {
        let mut is_client_cmd_running = false;

        loop {
            if is_client_cmd_running {
                let result = Self::wait_cmd_occupied_client(&mut system_rx, &mut return_rx, &mut register).await;
                if result.is_none() { break; }
                if result.unwrap() == true { is_client_cmd_running = false; }
            } else {
                let result = Self::wait_cmd_free_client(&mut client_rx, &mut system_rx, return_tx.clone(), &mut register).await;
                if result.is_none() { break; }
                if result.unwrap() == true { is_client_cmd_running = true; }
            }
        }
    }

    async fn wait_cmd_free_client(client_rx: &mut UnboundedReceiver<(ClientRegisterCommand, SuccessCallbackType)>,
                                  system_rx: &mut UnboundedReceiver<(SystemRegisterCommand, SystemCallbackType)>,
                                  return_tx: UnboundedSender<()>,
                                  register: &mut Box<dyn AtomicRegister>) -> Option<bool> {
        tokio::select! {
            biased;
            Some((cmd, cb)) = client_rx.recv() => {
                register.client_command(cmd, Box::new(|success| Box::pin(async move { let _ = return_tx.send(()); cb(success).await; }))).await;
                Some(true)
            },
            Some((cmd, cb)) = system_rx.recv() => {
                register.system_command(cmd).await;
                cb().await;
                Some(false)
            },
            else => None
        }
    }

    async fn wait_cmd_occupied_client(system_rx: &mut UnboundedReceiver<(SystemRegisterCommand, SystemCallbackType)>,
                                      return_rx: &mut UnboundedReceiver<()>,
                                      register: &mut Box<dyn AtomicRegister>) -> Option<bool> {
        tokio::select! {
            biased;
            Some(()) = return_rx.recv() => {
                Some(true)
            },
            Some((cmd, cb)) = system_rx.recv() => {
                register.system_command(cmd).await;
                cb().await;
                Some(false)
            },
            else => None
        }
    }
}