use std::cmp::{Ordering, PartialEq};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use uuid::Uuid;
use crate::{AtomicRegister, Broadcast, ClientRegisterCommand, ClientRegisterCommandContent, OperationReturn, OperationSuccess, ReadReturn, RegisterClient, SectorIdx, SectorsManager, SectorVec, SuccessCallbackType, SystemCommandHeader, SystemRegisterCommand, SystemRegisterCommandContent};

#[derive(Clone)]
pub struct Entry {
    value: SectorVec,
    timestamp: u64,
    write_rank: u8
}

impl Entry {
    fn ordering(a: &Entry, b: &Entry) -> Ordering {
        a.timestamp.cmp(&b.timestamp)
            .then(a.write_rank.cmp(&b.write_rank))
    }

    fn tuple(&self) -> (SectorVec, u64, u8) {
        (self.value.clone(), self.timestamp, self.write_rank)
    }
}

#[derive(PartialEq, Eq)]
enum Operation {
    Read(u64),
    Write(u64),
    Idle
}

#[derive(PartialEq, Eq)]
enum Phase {
    Acquiring,
    Updating
}

pub struct CustomRegister {
    rank: u8,
    sector_idx: SectorIdx,
    num_processes: u8,
    register_client: Arc<dyn RegisterClient>,
    sectors_manager: Arc<dyn SectorsManager>,

    state: Entry,
    read_list: HashMap<u8, Entry>,
    ack_list: HashSet<u8>,
    operation: Operation,
    op_id: Option<Uuid>,
    read_val: Option<SectorVec>,
    write_val: Option<SectorVec>,
    phase: Phase,

    callback: Option<SuccessCallbackType>
}

#[async_trait::async_trait]
impl AtomicRegister for CustomRegister {
    async fn client_command(&mut self, cmd: ClientRegisterCommand, success_callback: SuccessCallbackType) {
        if self.operation != Operation::Idle { panic!("The atomic register can only handle one read/write at a time"); }

        self.op_id = Some(Uuid::new_v4());
        self.read_list.clear();
        self.ack_list.clear();

        self.operation = match cmd.content {
            ClientRegisterCommandContent::Read => Operation::Read(cmd.header.request_identifier),
            ClientRegisterCommandContent::Write { data } => {
                self.write_val = Some(data);
                Operation::Write(cmd.header.request_identifier)
            }
        };

        self.callback = Some(success_callback);
        self.register_client.broadcast(Broadcast { cmd: Arc::new(self.gen_read_proc()) }).await;
    }

    async fn system_command(&mut self, cmd: SystemRegisterCommand) {
        let op_id = cmd.header.msg_ident;
        let sender_rank = cmd.header.process_identifier;
        
        match cmd.content {
            SystemRegisterCommandContent::ReadProc =>
                self.register_client.send(
                    crate::Send {
                        cmd: Arc::new(self.gen_value(op_id)),
                        target: sender_rank
                    }
                ).await,
            SystemRegisterCommandContent::Value { timestamp, write_rank, sector_data } =>
                self.receive_value(op_id, sender_rank, Entry { value: sector_data, timestamp, write_rank }).await,
            SystemRegisterCommandContent::WriteProc { timestamp, write_rank, data_to_write } => {
                let value = Entry { timestamp, write_rank, value: data_to_write };
                
                if Entry::ordering(&self.state, &value).is_lt() {
                    self.state = value;
                    self.sectors_manager.write(self.sector_idx, &self.state.tuple()).await;
                }

                self.register_client.send(
                    crate::Send { cmd: Arc::new(self.gen_ack(op_id)), target: sender_rank }
                ).await;
            }
            SystemRegisterCommandContent::Ack => 
                self.receive_ack(op_id, sender_rank).await
        }
    }
}

impl CustomRegister {
    async fn receive_value(&mut self, op_id: Uuid, sender_rank: u8, value: Entry) {
        if !self.op_id.is_some_and(|id| id == op_id) { return; }
        if self.phase != Phase::Acquiring { return; }

        self.read_list.insert(sender_rank, value);

        if self.read_list.len() <= self.num_processes as usize / 2 { return; }

        self.read_list.insert(self.rank, self.state.clone());

        let mut values: Vec<Entry> = Vec::new();
        self.read_list.values().for_each(|v| values.push(v.clone()));
        values.sort_by(|a, b| Entry::ordering(a, b));
        let latest = values[values.len() - 1].clone();

        self.read_val = Some(latest.value.clone());

        self.read_list.clear();
        self.ack_list.clear();

        self.phase = Phase::Updating;

        match self.operation {
            Operation::Read(_) => {
                let write_proc = self.gen_write_proc(
                    latest.value.clone(),
                    latest.timestamp,
                    latest.write_rank
                );
                self.register_client.broadcast(Broadcast { cmd: Arc::new(write_proc) }).await;
            },
            Operation::Write(_) => {
                self.state = Entry {
                    value: self.write_val.clone().unwrap(),
                    timestamp: latest.timestamp + 1,
                    write_rank: self.rank
                };

                self.sectors_manager.write(self.sector_idx, &self.state.tuple()).await;

                let write_proc = self.gen_write_proc(
                    self.state.value.clone(),
                    self.state.timestamp,
                    self.state.write_rank
                );

                self.register_client.broadcast(Broadcast { cmd: Arc::new(write_proc) }).await;
            }
            _ => {}
        }
    }

    async fn receive_ack(&mut self, op_id: Uuid, sender_rank: u8) {
        if !self.op_id.is_some_and(|id| id == op_id) { return; }
        if self.phase != Phase::Updating { return; }

        self.ack_list.insert(sender_rank);

        if self.ack_list.len() <= self.num_processes as usize / 2 { return; }
        if self.operation == Operation::Idle { return; }

        self.ack_list.clear();
        self.phase = Phase::Acquiring;

        let op_success = match self.operation {
            Operation::Read(request_identifier) => OperationSuccess {
                request_identifier,
                op_return: OperationReturn::Read(ReadReturn { read_data: self.read_val.clone().unwrap() })
            },
            Operation::Write(request_identifier) => OperationSuccess {
                request_identifier,
                op_return: OperationReturn::Write
            },
            _ => unreachable!()
        };

        self.callback.take().unwrap()(op_success).await;

        self.operation = Operation::Idle;
        self.op_id = None;
    }
}

impl CustomRegister {
    fn gen_read_proc(&self) -> SystemRegisterCommand {
        SystemRegisterCommand {
            header: SystemCommandHeader {
                process_identifier: self.rank,
                msg_ident: self.op_id.unwrap(),
                sector_idx: self.sector_idx
            },
            content: SystemRegisterCommandContent::ReadProc
        }
    }

    fn gen_value(&self, op_id: Uuid) -> SystemRegisterCommand {
        SystemRegisterCommand {
            header: SystemCommandHeader {
                process_identifier: self.rank,
                msg_ident: op_id,
                sector_idx: self.sector_idx,
            },
            content: SystemRegisterCommandContent::Value {
                timestamp: self.state.timestamp,
                write_rank: self.state.write_rank,
                sector_data: self.state.value.clone(),
            }
        }
    }

    fn gen_write_proc(&self, value: SectorVec, timestamp: u64, write_rank: u8) -> SystemRegisterCommand {
        SystemRegisterCommand {
            header: SystemCommandHeader {
                process_identifier: self.rank,
                msg_ident: self.op_id.unwrap(),
                sector_idx: self.sector_idx
            },
            content: SystemRegisterCommandContent::WriteProc {
                timestamp,
                write_rank,
                data_to_write: value
            }
        }
    }

    fn gen_ack(&self, op_id: Uuid) -> SystemRegisterCommand {
        SystemRegisterCommand {
            header: SystemCommandHeader {
                process_identifier: self.rank,
                msg_ident: op_id,
                sector_idx: self.sector_idx
            },
            content: SystemRegisterCommandContent::Ack
        }
    }

    pub async fn build(sector_idx: SectorIdx,
                       rank: u8,
                       num_processes: u8,
                       register_client: Arc<dyn RegisterClient>,
                       sectors_manager: Arc<dyn SectorsManager>) -> Self {
        let value = sectors_manager.read_data(sector_idx).await;
        let (timestamp, write_rank) = sectors_manager.read_metadata(sector_idx).await;

        Self {
            rank,
            sector_idx,
            num_processes,
            register_client,
            sectors_manager,
            state: Entry { value, timestamp, write_rank },
            read_list: HashMap::new(),
            ack_list: HashSet::new(),
            operation: Operation::Idle,
            op_id: None,
            read_val: None,
            write_val: None,
            phase: Phase::Acquiring,
            callback: None
        }
    }
}