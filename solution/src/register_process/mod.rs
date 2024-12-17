use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc};
use tokio::io::{AsyncRead};
use tokio::sync::Mutex;
use tokio::net::{TcpListener, TcpStream};
use tokio::net::tcp::OwnedWriteHalf;
use crate::{AtomicRegister, build_sectors_manager, ClientRegisterCommandContent, Configuration, deserialize_register_command, OperationReturn, RegisterClient, RegisterCommand, SectorIdx, SectorsManager};
use crate::{build_atomic_register};
use crate::receiver::command_listener_background;
use crate::transfer::client_response_transfer::{OperationResult, RegisterResponse, serialize_register_response};
use crate::register_client::stubborn_register_client::StubbornRegisterClient;

pub mod dispatcher;
pub mod receiver;

pub async fn run_register_process(config: Configuration) {
    let client_key = Arc::new(config.hmac_client_key);
    let system_key = Arc::new(config.hmac_system_key);
    let tcp_locations = config.public.tcp_locations;
    let rank = config.public.self_rank;
    let n_sectors = config.public.n_sectors;
    let dir = config.public.storage_dir;
    let process_count = tcp_locations.len() as u8;

    let location = tcp_locations[rank as usize - 1].clone();
    let system_key_clone = system_key.clone();
    let client_key_clone = client_key.clone();
    tokio::spawn(async move {
        let listener = TcpListener::bind((location.0.as_str(), location.1)).await.unwrap();
        let (client_cmd_tx, client_cmd_rx) = tokio::sync::mpsc::unbounded_channel();
        let (system_cmd_tx, system_cmd_rx) = tokio::sync::mpsc::unbounded_channel();

        loop {
            let (tcp_stream, _) = listener.accept().await.unwrap();

            tokio::spawn(
                command_listener_background(
                    tcp_stream,
                    n_sectors,
                    system_key_clone.clone(),
                    client_key_clone.clone(),
                    client_cmd_tx.clone(),
                    system_cmd_tx.clone())
            );
        }
    });


    let sectors_manager = build_sectors_manager(dir).await;
    let register_client = Arc::new(StubbornRegisterClient::build(tcp_locations.clone(), system_key.clone()).await);

    let location = tcp_locations[rank as usize - 1].clone();

    let system_key_clone = system_key.clone();
    tokio::spawn(async move {
        let listener = TcpListener::bind((location.0.as_str(), location.1)).await.unwrap();
        let active_registers: Arc<Mutex<HashMap<SectorIdx, Arc<Mutex<Box<dyn AtomicRegister>>>>>> = Arc::new(Mutex::new(HashMap::new()));

        loop {
            println!("Cazzi duri");
            let (tcp_stream, _) = listener.accept().await.unwrap();
            println!("Cazzi negri");

            tokio::spawn(background_process(
                tcp_stream,
                rank,
                process_count,
                n_sectors,
                register_client.clone(),
                sectors_manager.clone(),
                system_key_clone.clone(),
                client_key.clone(),
                active_registers.clone()
            ));
        }
    });
}

async fn background_process(
    stream: TcpStream,
    rank: u8,
    process_count: u8,
    n_sectors: u64,
    register_client: Arc<dyn RegisterClient>,
    sectors_manager: Arc<dyn SectorsManager>,
    system_key: Arc<[u8; 64]>,
    client_key: Arc<[u8; 32]>,
    active_registers: Arc<Mutex<HashMap<SectorIdx, Arc<Mutex<Box<dyn AtomicRegister>>>>>>) {

    let (mut read_stream, write_stream) = stream.into_split();
    let write_stream = Arc::new(Mutex::new(write_stream));

    loop {
        let (result, command, is_valid);

        {
            let mut buf = [0u8; 1];
            if let Ok(0) = read_stream.peek(&mut buf).await {
                return;
            }

            (command, is_valid) = extract_next_command(&mut read_stream, &system_key, &client_key).await;

            result = check_command(&command, is_valid, n_sectors, write_stream.clone(), client_key.deref()).await;
        }

        if result.is_err() {
            continue;
        }

        let idx = result.unwrap();

        let register;
        {
            let mut active_registers = active_registers.lock().await;
            register = if let Some(r) = active_registers.get_mut(&idx) {
                r.clone()
            } else {
                let r = build_atomic_register(
                    rank, idx, register_client.clone(), sectors_manager.clone(), process_count
                ).await;

                active_registers.insert(idx, Arc::new(Mutex::new(r)));

                active_registers.get_mut(&idx).unwrap().clone()
            };
        }

        println!("{:?}", command);
        match command {
            RegisterCommand::Client(cmd) => {
                let key = client_key.clone();
                let write_stream = write_stream.clone();

                register.lock().await.client_command(cmd, Box::new(|success| Box::pin(
                    async move {
                        let response = match &success.op_return {
                            OperationReturn::Read(_) => RegisterResponse::ReadResponse(OperationResult::Return(success)),
                            OperationReturn::Write => RegisterResponse::WriteResponse(OperationResult::Return(success)),
                        };

                        println!("{:?}", response);

                        serialize_register_response(&response, write_stream.lock().await.deref_mut(), key.as_slice()).await.unwrap();
                        println!("SUS");
                    }
                ))).await
            }
            RegisterCommand::System(cmd) => {
                register.lock().await.system_command(cmd).await;
            }
        }
    }
}

async fn extract_next_command(
    tcp_stream: &mut (dyn AsyncRead + Send + Unpin), system_key: &[u8; 64], client_key: &[u8; 32]
) -> (RegisterCommand, bool) {
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

async fn check_command(command: &RegisterCommand, valid: bool, n_sectors: u64, stream: Arc<Mutex<OwnedWriteHalf>>, client_key: &[u8]) -> Result<SectorIdx, ()> {
    let idx = extract_index(command);
    unimplemented!();
    /*
    return match command {
        RegisterCommand::Client(cmd) => {
            if !valid || idx >= n_sectors {
                let result = if !valid {
                    OperationResult::InvalidMac(idx)
                } else {
                    OperationResult::InvalidSector(idx)
                };

                let response = match cmd.content {
                    ClientRegisterCommandContent::Read =>
                        RegisterResponse::ReadResponse(result),
                    ClientRegisterCommandContent::Write { .. } =>
                        RegisterResponse::WriteResponse(result)
                };

                serialize_register_response(&response, stream.lock().await.deref_mut(), client_key).await.unwrap();
                return Err(());
            }

            Ok(idx)
        },
        RegisterCommand::System(_) => {
            if !valid || idx >= n_sectors {
                return Err(())
            }

            Ok(idx)
        }
    }*/
}