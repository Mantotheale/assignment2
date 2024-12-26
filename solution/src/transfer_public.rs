use std::collections::VecDeque;
use crate::{ClientCommandHeader, ClientRegisterCommand, ClientRegisterCommandContent, HmacSha256, MAGIC_NUMBER, RegisterCommand, SectorVec, SystemCommandHeader, SystemRegisterCommand, SystemRegisterCommandContent};
use std::io::{Error};
use std::io::ErrorKind::InvalidInput;
use hmac::Mac;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use uuid::Uuid;

const SECTOR_SIZE: usize = 4096;

struct Message {
    header: Vec<u8>,
    content: Vec<u8>,
    tag: Vec<u8>
}

impl Message {
    fn is_valid(&self, key: &[u8]) -> bool {
        let mut mac = HmacSha256::new_from_slice(key).unwrap();
        mac.update([self.header.clone(), self.content.clone()].concat().as_slice());
        mac.verify_slice(self.tag.as_slice()).is_ok()
    }
}

pub async fn deserialize_register_command(
    data: &mut (dyn AsyncRead + Send + Unpin),
    hmac_system_key: &[u8; 64],
    hmac_client_key: &[u8; 32],
) -> Result<(RegisterCommand, bool), Error> {
    let mut window = VecDeque::from([0u8; 4]);

    data.read_exact(window.make_contiguous()).await?;

    while window.make_contiguous() != MAGIC_NUMBER {
        window.push_back(data.read_u8().await?);
        window.pop_front();
    }

    let mut remainder = [0u8; 4];
    data.read_exact(&mut remainder).await?;

    let header = [window.make_contiguous(), remainder.as_slice()].concat();

    match remainder[3] {
        0x01 | 0x02 => deserialize_client_command(
            extract_bytes(
                data,
                48 + if remainder[3] == 0x01 { 0 } else { SECTOR_SIZE }, header
            ).await?,
            hmac_client_key,
            remainder[3]).await,
        0x03 | 0x04 | 0x05 | 0x06 => {
            deserialize_system_command(
                extract_bytes(
                    data,
                    match remainder[3] {
                        0x03 | 0x06 => 56,
                        0x04 | 0x05 => 56 + 16 + SECTOR_SIZE,
                        _ => unreachable!()
                    },
                    header).await?,
                hmac_system_key,
                remainder[2],
                remainder[3]).await
        },
        _ => Err(Error::from(InvalidInput))
    }
}

async fn extract_bytes(reader: &mut (dyn AsyncRead + Send + Unpin),
                       bytes: usize,
                       header: Vec<u8>) -> Result<Message, Error> {
    let mut msg_buf = vec![0u8; bytes];

    reader.read_exact(&mut msg_buf).await?;

    Ok(Message {
        header,
        content: msg_buf[..msg_buf.len() - 32].to_vec(),
        tag: msg_buf[msg_buf.len() - 32..].to_vec(),
    })
}

async fn client_command_content(reader: &mut (dyn AsyncRead + Send + Unpin), op_type: u8)
                                -> Result<ClientRegisterCommandContent, Error> {
    if op_type == 0x01 {
        Ok(ClientRegisterCommandContent::Read)
    } else {
        let mut data_buf = vec![0u8; SECTOR_SIZE];
        reader.read_exact(&mut data_buf).await?;

        Ok(ClientRegisterCommandContent::Write {
            data: SectorVec(data_buf.to_vec())
        })
    }
}

async fn deserialize_client_command(
    message: Message,
    hmac_key: &[u8; 32],
    op_type: u8
) -> Result<(RegisterCommand, bool), Error> {
    let reader: &mut (dyn AsyncRead + Send + Unpin) = &mut message.content.as_slice();

    let request_identifier = reader.read_u64().await?;
    let sector_idx = reader.read_u64().await?;

    Ok((
        RegisterCommand::Client(
            ClientRegisterCommand {
                header: ClientCommandHeader {
                    request_identifier,
                    sector_idx
                },
                content: client_command_content(reader, op_type).await?
            }
        ),
        message.is_valid(hmac_key)
    ))
}

async fn system_command_content(reader: &mut (dyn AsyncRead + Send + Unpin), op_type: u8)
                                -> Result<SystemRegisterCommandContent, Error> {
    match op_type {
        0x03 => Ok(SystemRegisterCommandContent::ReadProc),
        0x04 | 0x05 => {
            let timestamp = reader.read_u64().await?;

            let mut wr_buf = [0u8; 8];
            reader.read_exact(&mut wr_buf).await?;
            let write_rank = wr_buf[7];

            let mut data_buf = vec![0u8; SECTOR_SIZE];
            reader.read_exact(&mut data_buf).await?;

            Ok(if op_type == 0x04 {
                SystemRegisterCommandContent::Value { timestamp, write_rank,
                    sector_data: SectorVec(data_buf),
                }
            } else {
                SystemRegisterCommandContent::WriteProc { timestamp, write_rank,
                    data_to_write: SectorVec(data_buf)
                }
            })
        },
        0x06 => Ok(SystemRegisterCommandContent::Ack),
        _ => Err(Error::from(InvalidInput))
    }
}

async fn deserialize_system_command(
    message: Message,
    hmac_key: &[u8; 64],
    process_identifier: u8,
    op_type: u8
) -> Result<(RegisterCommand, bool), Error> {
    let reader: &mut (dyn AsyncRead + Send + Unpin) = &mut message.content.as_slice();

    let msg_ident = Uuid::from_u128(reader.read_u128().await?);
    let sector_idx = reader.read_u64().await?;

    Ok((
        RegisterCommand::System(
            SystemRegisterCommand {
                header: SystemCommandHeader {
                    process_identifier,
                    msg_ident,
                    sector_idx,
                },
                content: system_command_content(reader, op_type).await?
            }),
        message.is_valid(hmac_key)
    ))
}

pub async fn serialize_register_command(
    cmd: &RegisterCommand,
    writer: &mut (dyn AsyncWrite + Send + Unpin),
    hmac_key: &[u8],
) -> Result<(), Error> {
    let mut msg = match cmd {
        RegisterCommand::Client(cmd) => serialize_client_command(cmd),
        RegisterCommand::System(cmd) => serialize_system_command(cmd)
    };

    let mut mac = HmacSha256::new_from_slice(hmac_key).unwrap();
    mac.update(msg.as_slice());
    let tag = mac.finalize().into_bytes();

    msg = [msg, tag.to_vec()].concat();
    writer.write(msg.as_slice()).await?;

    Ok(())
}

fn serialize_client_command(command: &ClientRegisterCommand) -> Vec<u8> {
    let mut msg = [
        MAGIC_NUMBER.as_slice(),
        &[0u8; 3],
        &match command.content {
            ClientRegisterCommandContent::Read => 0x01u8,
            ClientRegisterCommandContent::Write { .. } => 0x02u8
        }.to_be_bytes(),
        &command.header.request_identifier.to_be_bytes(),
        &command.header.sector_idx.to_be_bytes()
    ].concat();

    if let ClientRegisterCommandContent::Write { data } = &command.content {
        msg = [msg, data.0.to_vec()].concat();
    }

    msg
}

fn serialize_system_command(command: &SystemRegisterCommand) -> Vec<u8> {
    let msg = [
        MAGIC_NUMBER.as_slice(),
        &[0u8; 2],
        &command.header.process_identifier.to_be_bytes(),
        &match command.content {
            SystemRegisterCommandContent::ReadProc => 0x03u8,
            SystemRegisterCommandContent::Value { .. } => 0x04u8,
            SystemRegisterCommandContent::WriteProc { .. } => 0x05u8,
            SystemRegisterCommandContent::Ack => 0x06u8
        }.to_be_bytes(),
        &command.header.msg_ident.into_bytes(),
        &command.header.sector_idx.to_be_bytes()
    ].concat();

    [
        msg,
        if let SystemRegisterCommandContent::Value { timestamp, write_rank, sector_data } = &command.content {
            [
                &timestamp.to_be_bytes(),
                [0u8; 7].as_slice(),
                &write_rank.to_be_bytes(),
                sector_data.0.as_slice()
            ].concat()
        } else if let SystemRegisterCommandContent::WriteProc { timestamp, write_rank, data_to_write} = &command.content {
            [
                &timestamp.to_be_bytes(),
                [0u8; 7].as_slice(),
                &write_rank.to_be_bytes(),
                data_to_write.0.as_slice()
            ].concat()
        } else {
            vec![]
        }
    ].concat()
}