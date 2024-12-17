use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use uuid::Uuid;
use crate::{AtomicRegister, Broadcast, ClientRegisterCommand, ClientRegisterCommandContent, OperationReturn, OperationSuccess, ReadReturn, RegisterClient, SectorIdx, SectorsManager, SectorVec, SystemCommandHeader, SystemRegisterCommand, SystemRegisterCommandContent};

pub mod atomic_register_public;

struct CustomAtomicRegister {
    self_ident: u8,
    sector_idx: SectorIdx,
    register_client: Arc<dyn RegisterClient>,
    sectors_manager: Arc<dyn SectorsManager>,
    processes_count: u8,

    timestamp: u64,
    write_rank: u8,
    value: SectorVec,
    read_list: HashMap<u8, (u64, u8, SectorVec)>,
    ack_list: HashSet<u8>,
    current_op: RegisterOperation,
    read_val: SectorVec,
    is_write_phase: bool,
}

enum RegisterOperation {
    Idle,
    Write(u64, Uuid, Option<SuccessCallbackType>, SectorVec),
    Read(u64, Uuid, Option<SuccessCallbackType>)
}

type SuccessCallbackType = Box<dyn FnOnce(OperationSuccess) -> Pin<Box<dyn Future<Output=()> + Send>> + Send + Sync>;

#[async_trait::async_trait]
impl AtomicRegister for CustomAtomicRegister {
    async fn client_command(&mut self, cmd: ClientRegisterCommand, success_callback: Box<dyn FnOnce(OperationSuccess) -> Pin<Box<dyn Future<Output=()> + Send>> + Send + Sync>) {
        let request_number = cmd.header.request_identifier;
        let op_id = Uuid::new_v4();

        self.read_list.clear();
        self.ack_list.clear();

        self.current_op = match cmd.content {
            ClientRegisterCommandContent::Read =>
                RegisterOperation::Read(request_number, op_id, Some(success_callback)),
            ClientRegisterCommandContent::Write { data } => {
                RegisterOperation::Write(request_number, op_id, Some(success_callback), data)
            }
        };

        self.register_client.broadcast(Broadcast { cmd: Arc::new(self.read_proc_cmd()) }).await;
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
                let is_correct_operation = match self.current_op {
                    RegisterOperation::Idle => false,
                    RegisterOperation::Read(_, id, _) => op_id == id,
                    RegisterOperation::Write(_, id, _, _) => op_id == id
                };

                if !is_correct_operation || self.is_write_phase {
                    return;
                }

                self.read_list.insert(sender, (timestamp, write_rank, sector_data.clone()));

                if self.read_list.len() as u8 > self.processes_count / 2 {
                    let latest = self.latest_value();

                    self.read_val = latest.2.clone();

                    self.read_list.clear();
                    self.ack_list.clear();
                    self.is_write_phase = true;

                    let command = match &self.current_op {
                        RegisterOperation::Read(_, _, _) => self.write_proc_cmd(op_id, latest.0, latest.1, &self.read_val),
                        RegisterOperation::Write(_, _, _, value) => {
                            self.timestamp = latest.0 + 1;
                            self.write_rank = self.self_ident;
                            self.value = value.clone();
                            self.sectors_manager.write(self.sector_idx, &(self.value.clone(), self.timestamp, self.write_rank)).await;

                            self.write_proc_cmd(op_id, self.timestamp, self.write_rank, value)
                        }
                        RegisterOperation::Idle => unreachable!()
                    };

                    self.register_client.broadcast(Broadcast { cmd: Arc::new(command) }).await;
                }
            }
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
            }
            SystemRegisterCommandContent::Ack => {
                let is_correct_operation = match self.current_op {
                    RegisterOperation::Idle => false,
                    RegisterOperation::Read(_, id, _) => op_id == id,
                    RegisterOperation::Write(_, id, _, _) => op_id == id
                };

                if !is_correct_operation || !self.is_write_phase {
                    return;
                }

                self.ack_list.insert(sender);

                if self.ack_list.len() as u8 > self.processes_count / 2 {
                    self.ack_list.clear();
                    self.is_write_phase = false;

                    let (op_return, callback, request_identifier) = match &mut self.current_op {
                        RegisterOperation::Read(id, _, callback) =>
                            (OperationReturn::Read(ReadReturn { read_data: self.value.clone() }), callback.take().unwrap(), id.clone()),
                        RegisterOperation::Write(id, _, callback, _) =>
                            (OperationReturn::Write, callback.take().unwrap(), id.clone()),
                        _ => unreachable!()
                    };

                    let response = OperationSuccess {
                        request_identifier,
                        op_return
                    };

                    self.current_op = RegisterOperation::Idle;

                    callback(response).await;
                }
            }
        }
    }
}

impl CustomAtomicRegister {
    fn read_proc_cmd(&self) -> SystemRegisterCommand {
        SystemRegisterCommand {
            header: SystemCommandHeader {
                process_identifier: self.self_ident,
                msg_ident: match self.current_op {
                    RegisterOperation::Write(_, op_id, _, _) | RegisterOperation::Read(_, op_id, _) => op_id,
                    _ => unreachable!()
                },
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

    fn latest_value(&self) -> (u64, u8, SectorVec) {
        let mut values: Vec<&(u64, u8, SectorVec)> = Vec::new();
        self.read_list.values().for_each(|v| values.push(v));
        values.sort_by(|a, b| b.0.cmp(&a.0).then(b.1.cmp(&a.1)));
        values[0].clone()
    }

    async fn build(
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
            read_list: HashMap::new(),
            ack_list: HashSet::new(),
            current_op: RegisterOperation::Idle,
            read_val: SectorVec(vec![]),
            is_write_phase: false,
        })
    }
}