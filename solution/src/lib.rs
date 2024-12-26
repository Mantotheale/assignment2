use std::future::Future;
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use hmac::Hmac;
use sha2::Sha256;
use tokio::io::AsyncRead;
use tokio::net::{TcpListener, TcpStream};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
pub use domain::*;
pub use atomic_register_public::*;
pub use register_client_public::*;
pub use sectors_manager_public::*;
pub use transfer_public::*;
use crate::stubborn_register_client::StubbornRegisterClient;
use crate::registers_manager::RegistersManager;
use crate::transfer::{Acknowledgment, MessageType, OperationError, OperationResult, RegisterResponse, serialize_ack, serialize_register_response};

mod domain;
mod transfer_public;
mod atomic_register_public;
mod sectors_manager_public;
mod register_client_public;

mod stubborn_register_client;
mod transfer;
mod registers_manager;

type HmacSha256 = Hmac<Sha256>;
type SuccessCallbackType = Box<dyn FnOnce(OperationSuccess) -> Pin<Box<dyn Future<Output=()> + std::marker::Send>> + std::marker::Send + Sync>;
type SystemCallbackType = Box<dyn FnOnce() -> Pin<Box<dyn Future<Output=()> + std::marker::Send>> + std::marker::Send + Sync>;

pub async fn run_register_process(config: Configuration) {
    let system_key = Arc::new(config.hmac_system_key);
    let client_key = Arc::new(config.hmac_client_key);

    let (system_tx, system_rx) = unbounded_channel();
    let (client_tx, client_rx) = unbounded_channel();

    let sectors_manager = build_sectors_manager(config.public.storage_dir).await;
    let register_client = Arc::new(StubbornRegisterClient::build(config.public.tcp_locations.clone(), system_key.clone(), config.public.self_rank, system_tx.clone()));

    let location = config.public.tcp_locations.get((config.public.self_rank - 1) as usize).unwrap();

    let listener = TcpListener::bind((location.0.as_str(), location.1)).await.unwrap();

    let registers_manager = RegistersManager::build(
        config.public.self_rank,
        register_client,
        sectors_manager,
        config.public.tcp_locations.len() as u8
    );

    tokio::spawn(listen_commands(system_rx, client_rx, registers_manager));

    loop {
        let (stream, _) = listener.accept().await.unwrap();

        tokio::spawn(handle_stream(
            stream,
            system_key.clone(),
            client_key.clone(),
            config.public.n_sectors,
            config.public.self_rank,
            system_tx.clone(),
            client_tx.clone()
        ));
    }
}

async fn listen_commands(mut system_queue: UnboundedReceiver<(SystemRegisterCommand, SystemCallbackType)>,
                         mut client_queue: UnboundedReceiver<(ClientRegisterCommand, SuccessCallbackType)>,
                         registers_manager: RegistersManager) {
    loop {
        tokio::select! {
            Some((cmd, cb)) = system_queue.recv() => {
                registers_manager.add_system_cmd(cmd, cb);
            },
            Some((cmd, cb)) = client_queue.recv() => {
                registers_manager.add_client_cmd(cmd, cb);
            }
        }
    }
}

async fn handle_stream(stream: TcpStream,
                       system_key: Arc<[u8; 64]>,
                       client_key: Arc<[u8; 32]>,
                       n_sectors: u64,
                       rank: u8,
                       system_tx: UnboundedSender<(SystemRegisterCommand, SystemCallbackType)>,
                       client_tx: UnboundedSender<(ClientRegisterCommand, SuccessCallbackType)>) {
    let (mut read_stream, write_stream) = stream.into_split();
    let (client_success_tx, client_success_rx) = unbounded_channel();
    let (system_success_tx, system_success_rx) = unbounded_channel();

    tokio::spawn(handle_write(
        client_success_rx, system_success_rx, write_stream, client_key.clone(), system_key.clone()
    ));

    loop {
        let mut buf = [0u8; 1];
        if let Ok(0) = read_stream.peek(&mut buf).await {
            return;
        }

        let (command, is_valid) = extract_next_command(&mut read_stream, &system_key, &client_key).await;
        let idx = extract_index(&command);

        if !is_command_ok(&command, is_valid, idx, n_sectors, client_success_tx.clone()).await {
            continue;
        }

        match command {
            RegisterCommand::Client(command) => {
                let client_success_tx = client_success_tx.clone();

                let callback: SuccessCallbackType = Box::new(|success| Box::pin(
                    async move {
                        let response = match &success.op_return {
                            OperationReturn::Read(_) => RegisterResponse::ReadResponse(OperationResult::Return(success)),
                            OperationReturn::Write => RegisterResponse::WriteResponse(OperationResult::Return(success))
                        };

                        client_success_tx.send(response).unwrap()
                    }
                ));

                client_tx.send((command, callback)).unwrap();
            },
            RegisterCommand::System(command) => {
                let system_success_tx = system_success_tx.clone();

                let ack = Arc::new(Acknowledgment {
                    msg_type: match command.content {
                        SystemRegisterCommandContent::ReadProc => MessageType::ReadProc,
                        SystemRegisterCommandContent::Value { .. } => MessageType::Value,
                        SystemRegisterCommandContent::WriteProc { .. } => MessageType::WriteProc,
                        SystemRegisterCommandContent::Ack => MessageType::Ack
                    },
                    process_rank: rank,
                    msg_ident: command.header.msg_ident,
                });

                let callback: SystemCallbackType = Box::new(|| Box::pin(async move {
                    system_success_tx.send(*ack.deref()).unwrap()
                }));

                system_tx.send((command, callback)).unwrap();
            }
        }
    }
}

async fn handle_write(mut client_success_rx: UnboundedReceiver<RegisterResponse>,
                      mut system_success_rx: UnboundedReceiver<Acknowledgment>,
                      mut write_stream: OwnedWriteHalf,
                      client_key: Arc<[u8; 32]>,
                      system_key: Arc<[u8; 64]>) {
    loop {
        tokio::select! {
            Some(response) = client_success_rx.recv() => {
                serialize_register_response(&response, &mut write_stream, client_key.deref()).await.unwrap();
            },
            Some(ack) = system_success_rx.recv() => {
                serialize_ack(&ack, &mut write_stream, system_key.deref()).await.unwrap();
            },
            else => break
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

async fn is_command_ok(command: &RegisterCommand,
                       is_hmac_valid: bool,
                       sector_idx: SectorIdx,
                       n_sectors: u64,
                       client_success_tx: UnboundedSender<RegisterResponse>) -> bool {
    if is_hmac_valid && sector_idx < n_sectors {
        return true;
    }

    if let RegisterCommand::Client(command) = &command  {
        let req_id = command.header.request_identifier;

        let err = if !is_hmac_valid {
            OperationError::InvalidMac(req_id)
        } else {
            OperationError::InvalidSector(req_id)
        };

        let err = OperationResult::Error(err);

        let response = match command.content {
            ClientRegisterCommandContent::Read => RegisterResponse::ReadResponse(err),
            ClientRegisterCommandContent::Write { .. } => RegisterResponse::WriteResponse(err)
        };

        client_success_tx.send(response).unwrap();
    }

    false
}