use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use hmac::Hmac;
use sha2::Sha256;
use tokio::io::AsyncRead;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
pub use domain::*;
pub use atomic_register_public::*;
pub use register_client_public::*;
pub use sectors_manager_public::*;
pub use transfer_public::*;
use crate::register_client::SimpleRegisterClient;
use crate::transfer::{OperationResult, RegisterResponse, serialize_register_response};

mod domain;
mod transfer_public;
mod atomic_register_public;
mod sectors_manager_public;
mod register_client_public;

mod register_client;
mod transfer;

type HmacSha256 = Hmac<Sha256>;

pub async fn run_register_process(config: Configuration) {
    let system_key = Arc::new(config.hmac_system_key);
    let client_key = Arc::new(config.hmac_client_key);

    let sectors_manager = build_sectors_manager(config.public.storage_dir).await;
    let register_client = Arc::new(SimpleRegisterClient::build(config.public.tcp_locations.clone(), system_key.clone()));

    let location = config.public.tcp_locations.get((config.public.self_rank - 1) as usize).unwrap();

    let listener = TcpListener::bind((location.0.as_str(), location.1)).await.unwrap();

    let active_registers: Arc<Mutex<HashMap<SectorIdx, Arc<Mutex<Box<dyn AtomicRegister>>>>>> = Arc::new(Mutex::new(HashMap::new()));

    loop {
        let (stream, _) = listener.accept().await.unwrap();
        println!("Ho accettato richiesta");

        tokio::spawn(handle_stream(
            stream,
            system_key.clone(),
            client_key.clone(),
            config.public.self_rank,
            register_client.clone(),
            sectors_manager.clone(),
            config.public.tcp_locations.len() as u8,
            active_registers.clone()
        ));
    }
}

async fn handle_stream(stream: TcpStream,
                       system_key: Arc<[u8; 64]>,
                       client_key: Arc<[u8; 32]>,
                       self_ident: u8,
                       register_client: Arc<dyn RegisterClient>,
                       sectors_manager: Arc<dyn SectorsManager>,
                       processes_count: u8,
                       active_registers: Arc<Mutex<HashMap<SectorIdx, Arc<Mutex<Box<dyn AtomicRegister>>>>>>) {
    let (mut read_stream, write_stream) = stream.into_split();
    let write_stream = Arc::new(Mutex::new(write_stream));

    loop {
        // todo: RICORDARSI DI GESTIRE CASI IDX O MAC INVALIDI
        let mut buf = [0u8; 1];
        if let Ok(0) = read_stream.peek(&mut buf).await {
            return;
        }

        println!("La stream non è chiusa");


        let (command, _) = extract_next_command(&mut read_stream, &system_key, &client_key.clone()).await;
        let idx = extract_index(&command);

        println!("Il comando è {:?}", command);

        let register;
        {
            let mut active_registers = active_registers.lock().await;

            register = if let Some(r) = active_registers.get(&idx) {
                println!("Il registro esisteva già");

                r.clone()
            } else {
                println!("Il registro non esisteva ancora");

                let r = Arc::new(Mutex::new(build_atomic_register(
                    self_ident, idx, register_client.clone(), sectors_manager.clone(), processes_count
                ).await));

                active_registers.insert(idx, r.clone());
                r
            };
        }

        println!("Ottenuto il registro");

        match command {
            RegisterCommand::Client(command) => {
                let client_key = client_key.clone();
                let write_stream = write_stream.clone();
                register.lock().await.deref_mut().client_command(command, Box::new(|success| Box::pin(
                    async move {
                        let response = match &success.op_return {
                            OperationReturn::Read(_) => RegisterResponse::ReadResponse(OperationResult::Return(success)),
                            OperationReturn::Write => RegisterResponse::WriteResponse(OperationResult::Return(success)),
                        };

                        println!("{:?}", response);

                        serialize_register_response(&response, write_stream.lock().await.deref_mut(), client_key.deref()).await.unwrap();
                        println!("SUS");
                    }
                ))).await
            },
            RegisterCommand::System(command) => {
                register.lock().await.deref_mut().system_command(command).await;
            }
        }
    }
}

async fn extract_next_command(tcp_stream: &mut (dyn AsyncRead + std::marker::Send + Unpin),
                              system_key: &[u8; 64],
                              client_key: &[u8; 32]) -> (RegisterCommand, bool) {
    loop {
        let result = deserialize_register_command(tcp_stream, system_key, client_key).await;

        if let Ok(command) = result {
            return command;
        }
    };
}

fn extract_index(command: &RegisterCommand) -> SectorIdx {
    match command {
        RegisterCommand::Client(cmd) => cmd.header.sector_idx,
        RegisterCommand::System(cmd) => cmd.header.sector_idx
    }
}