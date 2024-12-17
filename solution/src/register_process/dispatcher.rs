use std::collections::HashMap;
use std::ops::DerefMut;
use std::sync::Arc;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::Mutex;
use crate::{AtomicRegister, build_atomic_register, ClientRegisterCommand, OperationReturn, OperationSuccess, RegisterClient, SectorIdx, SectorsManager, SystemRegisterCommand};
use crate::transfer::client_response_transfer::{OperationResult, RegisterResponse, serialize_register_response};

#[derive(Clone)]
struct RegistersManager {
    active_registers: Arc<Mutex<HashMap<u64, Arc<Mutex<Box<dyn AtomicRegister>>>>>>,
    register_client: Arc<dyn RegisterClient>,
    sectors_manager: Arc<dyn SectorsManager>,
    processes_count: u8
}

impl RegistersManager {
    pub fn build(register_client: Arc<dyn RegisterClient>,
                 sectors_manager: Arc<dyn SectorsManager>,
                 processes_count: u8) -> RegistersManager {
        RegistersManager {
            active_registers: Arc::new(Mutex::new(HashMap::new())),
            register_client,
            sectors_manager,
            processes_count
        }
    }

    async fn get_register(&self, request_id: u64) -> Option<Arc<Mutex<Box<dyn AtomicRegister>>>> {
        let map =  self.active_registers.lock().await;
        let value = map.get(&request_id);

        match value {
            None => None,
            Some(v) => Some(v.clone())
        }
    }

    async fn add_register(&self, request_id: u64, sector_idx: SectorIdx, process_id: u8) {
        let r = build_atomic_register(
            process_id,
            sector_idx,
            self.register_client.clone(),
            self.sectors_manager.clone(),
            self.processes_count).await;

        self.active_registers.lock().await.insert(request_id, Arc::new(Mutex::new(r)));
    }

    async fn remove_register(&self, request_id: u64) {
        self.active_registers.lock().await.remove(&request_id);
    }
}

pub async fn client_dispatcher(mut cmd_receiver: UnboundedReceiver<(ClientRegisterCommand, Arc<Mutex<OwnedWriteHalf>>)>,
                               registers_manager: RegistersManager,
                               self_ident: u8,
                               key: Arc<[u8; 32]>) {
    loop {
        let command = cmd_receiver.recv().await;

        let (command, stream) = match command {
            None => break,
            Some(command) => command
        };

        let request_number = command.header.request_identifier;
        let sector_idx = command.header.sector_idx;

        let register = registers_manager.get_register(request_number).await;

        let register = register.unwrap_or({
            registers_manager.add_register(request_number, sector_idx, self_ident).await;
            registers_manager.get_register(request_number).await.unwrap()
        });

        let key = key.clone();
        let registers_manager = registers_manager.clone();
        register.lock().await.client_command(command, Box::new(|success: OperationSuccess| Box::pin(
            async move {
                let req_id = success.request_identifier;

                let response = match &success.op_return {
                    OperationReturn::Read(_) => RegisterResponse::ReadResponse(OperationResult::Return(success)),
                    OperationReturn::Write => RegisterResponse::WriteResponse(OperationResult::Return(success)),
                };

                println!("{:?}", response);

                serialize_register_response(&response, stream.lock().await.deref_mut(), key.as_slice()).await.unwrap();
                registers_manager.remove_register(req_id).await;
                println!("SUS");
            }
        ))).await;
    }
}

pub async fn system_dispatcher(mut cmd_receiver: UnboundedReceiver<SystemRegisterCommand>,
                               registers_manager: RegistersManager,
                               self_ident: u8) {
    loop {
        let command = cmd_receiver.recv().await;

        let command = match command {
            None => break,
            Some(command) => command
        };

        let request_number = command.header.request_identifier;
        let sector_idx = command.header.sector_idx;

        let register = registers_manager.get_register(request_number).await;

        let register = register.unwrap_or({
            registers_manager.add_register(request_number, sector_idx, self_ident).await;
            registers_manager.get_register(request_number).await.unwrap()
        });

        let key = key.clone();
        let registers_manager = registers_manager.clone();
        register.lock().await.client_command(command, Box::new(|success: OperationSuccess| Box::pin(
            async move {
                let req_id = success.request_identifier;

                let response = match &success.op_return {
                    OperationReturn::Read(_) => RegisterResponse::ReadResponse(OperationResult::Return(success)),
                    OperationReturn::Write => RegisterResponse::WriteResponse(OperationResult::Return(success)),
                };

                println!("{:?}", response);

                serialize_register_response(&response, stream.lock().await.deref_mut(), key.as_slice()).await.unwrap();
                registers_manager.remove_register(req_id).await;
                println!("SUS");
            }
        ))).await;
    }
}