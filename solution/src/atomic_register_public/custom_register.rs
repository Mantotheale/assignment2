use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::select;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use crate::{AtomicRegister, ClientRegisterCommand, OperationReturn, OperationSuccess, RegisterClient, SectorIdx, SectorsManager, SuccessCallbackType, SystemRegisterCommand, SystemRegisterCommandContent};
use crate::atomic_register_public::cs_register::{CSRegister, Entry};

struct ClientOperationData {
    id: u64,
    callback: SuccessCallbackType
}

pub struct RegisterHandler {
    client_tx: UnboundedSender<(ClientRegisterCommand, SuccessCallbackType)>,
    system_tx: UnboundedSender<SystemRegisterCommand>,
}

#[async_trait::async_trait]
impl AtomicRegister for RegisterHandler {
    async fn client_command(&mut self, cmd: ClientRegisterCommand, success_callback: Box<dyn FnOnce(OperationSuccess) -> Pin<Box<dyn Future<Output=()> + Send>> + Send + Sync>) {
        self.client_tx.send((cmd, success_callback)).unwrap();
    }

    async fn system_command(&mut self, cmd: SystemRegisterCommand) {
        self.system_tx.send(cmd).unwrap()
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

        let register = CSRegister::build(
            sector_idx, rank, num_processes, register_client, sectors_manager, return_tx
        ).await;

        tokio::spawn(Self::handler_background(
           client_rx, system_rx, return_rx, register
        ));

        Self {
            client_tx,
            system_tx
        }
    }

    async fn handler_background(mut client_rx: UnboundedReceiver<(ClientRegisterCommand, SuccessCallbackType)>,
                                mut system_rx: UnboundedReceiver<SystemRegisterCommand>,
                                mut return_rx: UnboundedReceiver<OperationReturn>,
                                mut register: CSRegister) {
        let mut client_op: Option<ClientOperationData> = None;

        loop {
            match &mut client_op {
                None => {
                    select! {
                        biased;
                        Some((cmd, cb)) = client_rx.recv() => {
                            let op_data = ClientOperationData {
                                id: cmd.header.request_identifier,
                                callback: cb
                            };

                            client_op = Some(op_data);
                            register.receive_client_cmd(cmd.content).await;
                        },
                        Some(cmd) = system_rx.recv() =>
                            RegisterHandler::handle_system_cmd(cmd, &mut register).await
                    }
                }
                Some(_) => {
                    select! {
                        biased;
                        Some(op_return) = return_rx.recv() => {
                            let op = client_op.take().unwrap();

                            let success = OperationSuccess {
                                request_identifier: op.id,
                                op_return
                            };

                            let cb = op.callback;
                            cb(success).await;
                        },
                        Some(cmd) = system_rx.recv() => {
                            RegisterHandler::handle_system_cmd(cmd, &mut register).await;
                        },
                    }
                }
            }
        }
    }

    async fn handle_system_cmd(cmd: SystemRegisterCommand, register: &mut CSRegister) {
        let op_id = cmd.header.msg_ident;
        let sender_rank = cmd.header.process_identifier;

        match cmd.content {
            SystemRegisterCommandContent::ReadProc =>
                register.receive_read_proc(op_id, sender_rank).await,
            SystemRegisterCommandContent::Value{timestamp, write_rank, sector_data } =>
                register.receive_value(op_id, sender_rank, Entry::new(sector_data, timestamp, write_rank)).await,
            SystemRegisterCommandContent::WriteProc{ timestamp, write_rank, data_to_write } =>
                register.receive_write_proc( Entry::new(data_to_write, timestamp, write_rank), op_id, sender_rank).await,
            SystemRegisterCommandContent::Ack =>
                register.receive_ack(op_id, sender_rank).await
        }
    }
}