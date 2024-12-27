use std::ops::Deref;
use std::sync::Arc;
use tokio::net::tcp::OwnedReadHalf;
use tokio::net::TcpStream;
use crate::{ClientRegisterCommandContent, deserialize_register_command, RegisterCommand, SectorIdx, SuccessCallbackType};
use crate::registers_dispatcher::RegistersDispatcher;
use crate::tcp_writer::TcpWriter;
use crate::transfer::{from_success_to_response, OperationError, OperationResult, RegisterResponse};

pub struct StreamHandler {
    read_stream: OwnedReadHalf,
    tcp_writer: TcpWriter,
    system_key: Arc<[u8; 64]>,
    client_key: Arc<[u8; 32]>,
    n_sectors: u64,
    registers_dispatcher: RegistersDispatcher
}

impl StreamHandler {
    pub fn build(stream: TcpStream,
                 system_key: Arc<[u8; 64]>,
                 client_key: Arc<[u8; 32]>,
                 n_sectors: u64,
                 registers_dispatcher: RegistersDispatcher) -> Self {
        let (read_stream, write_stream) = stream.into_split();
        let tcp_writer = TcpWriter::build(write_stream, system_key.clone(), client_key.clone());

        Self { read_stream, tcp_writer, system_key, client_key, n_sectors, registers_dispatcher }
    }

    pub async fn run(&mut self) {
        loop {
            let mut buf = [0u8; 1];
            if let Ok(0) = self.read_stream.peek(&mut buf).await {
                return;
            }

            let (command, is_valid) = self.extract_next_command().await;
            let sector_idx = Self::extract_index(&command);

            if !self.is_command_ok(&command, is_valid, sector_idx) {
                continue;
            }

            match command {
                RegisterCommand::Client(command) => {
                    let tcp_writer = self.tcp_writer.clone();

                    let callback: SuccessCallbackType = Box::new(|success| Box::pin(
                        async move { tcp_writer.send_response(from_success_to_response(success)); }
                    ));

                    self.registers_dispatcher.add_client_cmd(command, callback);
                },
                RegisterCommand::System(command) => {
                    self.registers_dispatcher.add_system_cmd(command, Some(self.tcp_writer.clone()));
                }
            }
        }
    }

    async fn extract_next_command(&mut self) -> (RegisterCommand, bool) {
        loop {
            let result = deserialize_register_command(
                &mut self.read_stream, self.system_key.deref(), self.client_key.deref()
            ).await;

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

    fn is_command_ok(&self, command: &RegisterCommand, is_hmac_valid: bool, sector_idx: SectorIdx) -> bool {
        if is_hmac_valid && sector_idx < self.n_sectors {
            return true;
        }

        if let RegisterCommand::Client(command) = &command  {
            let req_id = command.header.request_identifier;

            let err = OperationResult::Error(
                if !is_hmac_valid {
                    OperationError::InvalidMac(req_id)
                } else {
                    OperationError::InvalidSector(req_id)
                }
            );

            let response = match command.content {
                ClientRegisterCommandContent::Read => RegisterResponse::ReadResponse(err),
                ClientRegisterCommandContent::Write { .. } => RegisterResponse::WriteResponse(err)
            };

            self.tcp_writer.send_response(response);
        }

        false
    }
}