use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use uuid::Uuid;
use crate::{AtomicRegister, Broadcast, ClientRegisterCommand, ClientRegisterCommandContent, OperationReturn, OperationSuccess, ReadReturn, RegisterClient, SectorIdx, SectorsManager, SectorVec, SystemCommandHeader, SystemRegisterCommand, SystemRegisterCommandContent};

struct RegisterOperation {
    request_number: u64,
    callback: Option<SuccessCallbackType>,
    op_type: OperationType,
    is_write_phase: bool,
    read_list: HashMap<u8, (u64, u8, SectorVec)>,
    ack_list: HashSet<u8>,
}

#[derive(Clone)]
enum OperationType {
    Read(Option<SectorVec>),
    Write(SectorVec)
}

pub struct CustomAtomicRegister {
    self_ident: u8,
    sector_idx: SectorIdx,
    register_client: Arc<dyn RegisterClient>,
    sectors_manager: Arc<dyn SectorsManager>,
    processes_count: u8,

    timestamp: u64,
    write_rank: u8,
    value: SectorVec,

    active_operations: HashMap<Uuid, RegisterOperation>
}

type SuccessCallbackType = Box<dyn FnOnce(OperationSuccess) -> Pin<Box<dyn Future<Output=()> + Send>> + Send + Sync>;

#[async_trait::async_trait]
impl AtomicRegister for CustomAtomicRegister {
    async fn client_command(&mut self, cmd: ClientRegisterCommand, success_callback: Box<dyn FnOnce(OperationSuccess) -> Pin<Box<dyn Future<Output=()> + Send>> + Send + Sync>) {
        let op_id = Uuid::new_v4();

        self.active_operations.insert(
            op_id.clone(),
            RegisterOperation {
                request_number: cmd.header.request_identifier,
                callback: Some(success_callback),
                op_type: match cmd.content {
                    ClientRegisterCommandContent::Read =>
                        OperationType::Read(None),
                    ClientRegisterCommandContent::Write { data } =>
                        OperationType::Write(data)
                },
                is_write_phase: false,
                read_list: HashMap::new(),
                ack_list: HashSet::new()
            });

        self.register_client.broadcast(Broadcast { cmd: Arc::new(self.read_proc_cmd(&op_id)) }).await;
    }

    async fn system_command(&mut self, cmd: SystemRegisterCommand) {
        let op_id = cmd.header.msg_ident;
        let sender = cmd.header.process_identifier;

        match cmd.content {
            SystemRegisterCommandContent::ReadProc =>
                self.register_client.send(crate::Send {
                    cmd: Arc::new(self.value_cmd(&op_id)),
                    target: sender
                }).await,
            SystemRegisterCommandContent::Value { timestamp, write_rank, sector_data } => {
                let op = self.active_operations.get_mut(&op_id);
                if op.is_none() { return; }
                let op = op.unwrap();

                if op.is_write_phase { return; }

                op.read_list.insert(sender, (timestamp, write_rank, sector_data.clone()));

                if op.read_list.len() as u8 > self.processes_count / 2 {
                    let mut values = Vec::new();

                    op.read_list.values().for_each(|v| values.push(v));
                    values.sort_by(|a, b| b.0.cmp(&a.0).then(b.1.cmp(&a.1)));
                    //values.get(0).map(|v| (*v).clone())
                    let latest = values[0].clone();

                    op.read_list.clear();
                    op.ack_list.clear();
                    op.is_write_phase = true;

                    let command = match op.op_type.clone() {
                        OperationType::Read(_) => {
                            let read_val = latest.2;
                            op.op_type = OperationType::Read(Some(read_val.clone()));
                            self.write_proc_cmd(op_id, latest.0, latest.1, &read_val)
                        },
                        OperationType::Write(value) => {
                            self.timestamp = latest.0 + 1;
                            self.write_rank = self.self_ident;
                            self.value = value.clone();
                            self.sectors_manager.write(self.sector_idx, &(self.value.clone(), self.timestamp, self.write_rank)).await;

                            self.write_proc_cmd(op_id, self.timestamp, self.write_rank, &value)
                        }
                    };

                    self.register_client.broadcast(Broadcast { cmd: Arc::new(command) }).await;
                }
            },
            SystemRegisterCommandContent::WriteProc { timestamp, write_rank, data_to_write } => {
                if self.timestamp.cmp(&timestamp).then(self.self_ident.cmp(&write_rank)).is_lt() {
                    self.timestamp = timestamp;
                    self.write_rank = write_rank;
                    self.value = data_to_write;

                    self.sectors_manager.write(self.sector_idx, &(self.value.clone(), self.timestamp, self.write_rank)).await;
                }

                self.register_client.send(crate::Send {
                    cmd: Arc::new(self.ack_cmd(op_id)),
                    target: cmd.header.process_identifier
                }).await;
            },
            SystemRegisterCommandContent::Ack => {
                let op = self.active_operations.get_mut(&op_id);
                if op.is_none() { return; }
                let op = op.unwrap();

                if !op.is_write_phase { return; }

                op.ack_list.insert(sender);

                if op.ack_list.len() as u8 > self.processes_count / 2 {
                    op.ack_list.clear();
                    op.is_write_phase = false;

                    let op_return = match &op.op_type {
                        OperationType::Read(value) =>
                            OperationReturn::Read(ReadReturn { read_data: value.clone().unwrap() }),
                        OperationType::Write(_) =>
                            OperationReturn::Write
                    };

                    let response = OperationSuccess {
                        request_identifier: op.request_number,
                        op_return
                    };

                    op.callback.take().unwrap()(response).await;

                    self.active_operations.remove(&op_id);
                }
            }
        }
    }
}

impl CustomAtomicRegister {
    fn read_proc_cmd(&self, op_id: &Uuid) -> SystemRegisterCommand {
        SystemRegisterCommand {
            header: SystemCommandHeader {
                process_identifier: self.self_ident,
                msg_ident: *op_id,
                sector_idx: self.sector_idx
            },
            content: SystemRegisterCommandContent::ReadProc
        }
    }

    fn value_cmd(&self, op_id: &Uuid) -> SystemRegisterCommand {
        SystemRegisterCommand {
            header: SystemCommandHeader {
                process_identifier: self.self_ident,
                msg_ident: *op_id,
                sector_idx: self.sector_idx,
            },
            content: SystemRegisterCommandContent::Value {
                timestamp: self.timestamp,
                write_rank: self.write_rank,
                sector_data: self.value.clone(),
            }
        }
    }

    fn write_proc_cmd(&self, op_id: Uuid, timestamp: u64, write_rank: u8, data: &SectorVec) -> SystemRegisterCommand {
        SystemRegisterCommand {
            header: SystemCommandHeader {
                process_identifier: self.self_ident,
                msg_ident: op_id,
                sector_idx: self.sector_idx,
            },
            content: SystemRegisterCommandContent::WriteProc {
                timestamp,
                write_rank,
                data_to_write: data.clone()
            }
        }
    }

    fn ack_cmd(&self, op_id: Uuid) -> SystemRegisterCommand {
        SystemRegisterCommand {
            header: SystemCommandHeader {
                process_identifier: self.self_ident,
                msg_ident: op_id,
                sector_idx: self.sector_idx,
            },
            content: SystemRegisterCommandContent::Ack
        }
    }

    pub async fn build(
        self_ident: u8,
        sector_idx: SectorIdx,
        register_client: Arc<dyn RegisterClient>,
        sectors_manager: Arc<dyn SectorsManager>,
        processes_count: u8
    ) -> Box<dyn AtomicRegister> {
        let (timestamp, write_rank) = sectors_manager.read_metadata(sector_idx).await;
        let value = sectors_manager.read_data(sector_idx).await;

        Box::new(CustomAtomicRegister {
            self_ident,
            sector_idx,
            register_client,
            sectors_manager,
            processes_count,
            timestamp,
            write_rank,
            value,
            active_operations: HashMap::new()
        })
    }
}