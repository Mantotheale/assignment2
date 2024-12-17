use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::Mutex;
use crate::{ClientRegisterCommand, ClientRegisterCommandContent, deserialize_register_command, RegisterCommand, SystemRegisterCommand};
use crate::transfer::client_response_transfer::{OperationError, OperationResult, RegisterResponse, serialize_register_response};

pub async fn command_listener_background(stream: TcpStream,
                                     n_sectors: u64,
                                     system_key: Arc<[u8; 64]>,
                                     client_key: Arc<[u8; 32]>,
                                     client_sender: UnboundedSender<ClientRegisterCommand>,
                                     system_sender: UnboundedSender<SystemRegisterCommand>) {
    let (mut read_stream, write_stream) = stream.into_split();
    let write_stream = Arc::new(Mutex::new(write_stream));

    loop {
        let result = wait_next_command(&mut read_stream, system_key.clone(), client_key.clone()).await;

        let (command, is_valid) = match result {
            None => break,
            Some(x) => x
        };

        if !is_command_valid(&command, is_valid, n_sectors, write_stream.clone(), client_key.clone()).await {
            continue;
        }

        match command {
            RegisterCommand::Client(command) => client_sender.send(command).unwrap(),
            RegisterCommand::System(command) => system_sender.send(command).unwrap()
        };
    }
}

async fn wait_next_command(stream: &mut OwnedReadHalf, system_key: Arc<[u8; 64]>, client_key: Arc<[u8; 32]>) -> Option<(RegisterCommand, bool)> {
    loop {
        let mut buf = [0u8, 1];
        if let Ok(0) = stream.peek(&mut buf).await {
            return None;
        }

        let result = deserialize_register_command(stream, system_key.deref(), client_key.deref()).await;

        if let Ok((command, is_valid)) = result {
            return Some((command, is_valid));
        }
    };
}

async fn is_command_valid(command: &RegisterCommand,
                    is_valid: bool,
                    n_sectors: u64,
                    stream: Arc<Mutex<OwnedWriteHalf>>,
                    client_key: Arc<[u8; 32]>) -> bool {
    match command {
        RegisterCommand::Client(command) => {
            let result = validate_client_command(&command, is_valid, n_sectors);

            if result.is_err() {
                let err = OperationResult::Error(result.err().unwrap());

                let response = match command.content {
                    ClientRegisterCommandContent::Read =>
                        RegisterResponse::ReadResponse(err),
                    ClientRegisterCommandContent::Write { .. } =>
                        RegisterResponse::WriteResponse(err)
                };

                serialize_register_response(&response, stream.lock().await.deref_mut(), client_key.deref()).await.unwrap();
                false
            } else { true }
        }
        RegisterCommand::System(command) => {
            if !is_valid { false }
            else if command.header.sector_idx > n_sectors { false }
            else { true }
        }
    }
}

fn validate_client_command(command: &ClientRegisterCommand, hmac_is_valid: bool, n_sectors: u64)
    -> Result<(), OperationError> {
    let request_number = command.header.request_identifier;
    let idx = command.header.sector_idx;

    if !hmac_is_valid {
        return Err(OperationError::InvalidMac(request_number));
    }

    if idx >= n_sectors {
        return Err(OperationError::InvalidSector(request_number));
    }

    return Ok(());
}