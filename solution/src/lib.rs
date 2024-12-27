use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use hmac::Hmac;
use sha2::Sha256;
use tokio::io::AsyncRead;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::unbounded_channel;
pub use domain::*;
pub use atomic_register_public::*;
pub use register_client_public::*;
pub use sectors_manager_public::*;
pub use transfer_public::*;
use crate::stubborn_register_client::StubbornRegisterClient;
use crate::registers_manager::RegistersDispatcher;
use crate::tcp_writer::TcpWriter;
use crate::transfer::{OperationError, OperationResult, RegisterResponse};

mod domain;
mod transfer_public;
mod atomic_register_public;
mod sectors_manager_public;
mod register_client_public;

mod stubborn_register_client;
mod transfer;
mod registers_manager;
mod tcp_writer;

type HmacSha256 = Hmac<Sha256>;
type SuccessCallbackType = Box<dyn FnOnce(OperationSuccess) -> Pin<Box<dyn Future<Output=()> + std::marker::Send>> + std::marker::Send + Sync>;

pub async fn run_register_process(config: Configuration) {
    let system_key = Arc::new(config.hmac_system_key);
    let client_key = Arc::new(config.hmac_client_key);

    let (system_tx, system_rx) = unbounded_channel();

    let sectors_manager = build_sectors_manager(config.public.storage_dir).await;
    let register_client = Arc::new(StubbornRegisterClient::build(config.public.tcp_locations.clone(), system_key.clone(), config.public.self_rank, system_tx.clone()));

    let location = config.public.tcp_locations.get((config.public.self_rank - 1) as usize).unwrap();

    let listener = TcpListener::bind((location.0.as_str(), location.1)).await.unwrap();

    let registers_dispatcher = RegistersDispatcher::build(
        system_tx,
        system_rx,
        config.public.self_rank,
        register_client,
        sectors_manager,
        config.public.tcp_locations.len() as u8
    );

    loop {
        let (stream, _) = listener.accept().await.unwrap();

        tokio::spawn(handle_stream(
            stream,
            system_key.clone(),
            client_key.clone(),
            config.public.n_sectors,
            registers_dispatcher.clone()
        ));
    }
}

async fn handle_stream(stream: TcpStream,
                       system_key: Arc<[u8; 64]>,
                       client_key: Arc<[u8; 32]>,
                       n_sectors: u64,
                       registers_dispatcher: RegistersDispatcher) {
    let (mut read_stream, write_stream) = stream.into_split();
    let tcp_writer = TcpWriter::build(write_stream, system_key.clone(), client_key.clone());

    loop {
        let mut buf = [0u8; 1];
        if let Ok(0) = read_stream.peek(&mut buf).await {
            return;
        }

        let (command, is_valid) = extract_next_command(&mut read_stream, &system_key, &client_key).await;
        let idx = extract_index(&command);

        if !is_command_ok(&command, is_valid, idx, n_sectors, tcp_writer.clone()) {
            continue;
        }

        match command {
            RegisterCommand::Client(command) => {
                let tcp_writer = tcp_writer.clone();

                let callback: SuccessCallbackType = Box::new(|success| Box::pin(
                    async move {
                        let response = match &success.op_return {
                            OperationReturn::Read(_) => RegisterResponse::ReadResponse(OperationResult::Return(success)),
                            OperationReturn::Write => RegisterResponse::WriteResponse(OperationResult::Return(success))
                        };

                        tcp_writer.send_response(response);
                    }
                ));

                registers_dispatcher.add_client_cmd(command, callback);
            },
            RegisterCommand::System(command) => {
                registers_dispatcher.add_system_cmd(command, Some(tcp_writer.clone()));
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

fn is_command_ok(command: &RegisterCommand,
                       is_hmac_valid: bool,
                       sector_idx: SectorIdx,
                       n_sectors: u64,
                       tcp_writer: TcpWriter) -> bool {
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

        tcp_writer.send_response(response);
    }

    false
}