use std::collections::VecDeque;
use std::io::Error;
use std::io::ErrorKind::InvalidInput;
use hmac::Mac;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use uuid::Uuid;
use crate::{HmacSha256, MAGIC_NUMBER, OperationReturn, OperationSuccess, StatusCode, SystemRegisterCommand, SystemRegisterCommandContent};

#[derive(Debug, Clone)]
pub enum RegisterResponse {
    ReadResponse(OperationResult),
    WriteResponse(OperationResult)
}

#[derive(Debug, Clone)]
pub enum OperationResult {
    Return(OperationSuccess),
    Error(OperationError)
}

#[derive(Debug, Clone)]
pub enum OperationError {
    InvalidMac(u64),
    InvalidSector(u64)
}

pub fn from_success_to_response(success: OperationSuccess) -> RegisterResponse {
    match &success.op_return {
        OperationReturn::Read(_) => RegisterResponse::ReadResponse(OperationResult::Return(success)),
        OperationReturn::Write => RegisterResponse::WriteResponse(OperationResult::Return(success))
    }
}

pub async fn serialize_register_response(response: &RegisterResponse,
                                         writer: &mut (dyn AsyncWrite + Send + Unpin),
                                         hmac_key: &[u8]) -> Result<(), Error> {
    let mut msg = serialize_content(response);

    let mut mac = HmacSha256::new_from_slice(hmac_key).unwrap();
    mac.update(msg.as_slice());
    let tag = mac.finalize().into_bytes();

    msg = [msg, tag.to_vec()].concat();
    writer.write(msg.as_slice()).await?;

    Ok(())
}

fn serialize_content(response: &RegisterResponse) -> Vec<u8> {
    let (msg_type, result) = match response {
        RegisterResponse::ReadResponse(result) => (0x41u8, result),
        RegisterResponse::WriteResponse(result) => (0x42u8, result)
    };

    let (status_code, request_number) = match result {
        OperationResult::Return(content) => (StatusCode::Ok, content.request_identifier),
        OperationResult::Error(err) => {
            match err {
                OperationError::InvalidMac(req_num) => (StatusCode::AuthFailure, req_num.clone()),
                OperationError::InvalidSector(req_num) => (StatusCode::InvalidSectorIndex, req_num.clone())
            }
        }
    };

    let mut msg = [
        MAGIC_NUMBER.as_slice(),
        &[0u8; 2],
        &(status_code as u8).to_be_bytes(),
        &msg_type.to_be_bytes(),
        &request_number.to_be_bytes()
    ].concat();

    if let OperationResult::Return(op) = result {
        if let OperationReturn::Read(content) = &op.op_return {
            msg = [msg, content.read_data.0.to_vec()].concat();
        }
    }

    msg
}

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
pub struct Acknowledgment {
    msg_type: MessageType,
    process_rank: u8,
    msg_ident: Uuid,
}

impl Acknowledgment {
    pub fn from_cmd(cmd: SystemRegisterCommand, rank: u8) -> Self {
        Self {
            msg_type: MessageType::from_content(cmd.content),
            process_rank: rank,
            msg_ident: cmd.header.msg_ident
        }
    }
}

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
enum MessageType {
    ReadProc,
    Value,
    WriteProc,
    Ack
}

impl MessageType {
    fn from_content(content: SystemRegisterCommandContent) -> Self {
        match content {
            SystemRegisterCommandContent::ReadProc => MessageType::ReadProc,
            SystemRegisterCommandContent::Value { .. } => MessageType::Value,
            SystemRegisterCommandContent::WriteProc { .. } => MessageType::WriteProc,
            SystemRegisterCommandContent::Ack => MessageType::Ack,
        }
    }
}

pub async fn deserialize_ack(
    data: &mut (dyn AsyncRead + Send + Unpin),
    hmac_key: &[u8; 64],
) -> Result<(Acknowledgment, bool), Error> {
    let mut window = VecDeque::from([0u8; 4]);

    data.read_exact(window.make_contiguous()).await?;

    while window.make_contiguous() != MAGIC_NUMBER {
        window.push_back(data.read_u8().await?);
        window.pop_front();
    }

    let mut padding = [0u8; 2];
    data.read_exact(&mut padding).await?;

    let process_rank = data.read_u8().await?;
    let msg_type_raw = data.read_u8().await?;

    let msg_type = match msg_type_raw {
        0x43 => MessageType::ReadProc,
        0x44 => MessageType::Value,
        0x45 => MessageType::WriteProc,
        0x46 => MessageType::Ack,
        _ => return Err(Error::from(InvalidInput))
    };

    let mut msg_content = [0u8; 48];
    data.read_exact(&mut msg_content).await?;

    let msg_ident = u128::from_be_bytes(msg_content[..16].try_into().unwrap());
    let hmac_tag = &msg_content[16..];

    let content = [
        window.make_contiguous(),
        padding.as_slice(),
        &process_rank.to_be_bytes(),
        &msg_type_raw.to_be_bytes(),
        &msg_ident.to_be_bytes()
    ].concat();

    let ack = Acknowledgment {
        process_rank,
        msg_ident: Uuid::from_u128(msg_ident),
        msg_type
    };

    let mut mac = HmacSha256::new_from_slice(hmac_key).unwrap();
    mac.update(content.as_slice());
    let is_valid = mac.verify_slice(&hmac_tag).is_ok();

    Ok((ack, is_valid))
}

pub async fn serialize_ack(
    ack: &Acknowledgment,
    writer: &mut (dyn AsyncWrite + Send + Unpin),
    hmac_key: &[u8],
) -> Result<(), Error> {
    let msg_type: u8 = match ack.msg_type {
        MessageType::ReadProc => 0x43,
        MessageType::Value => 0x44,
        MessageType::WriteProc => 0x45,
        MessageType::Ack => 0x46
    };

    let mut msg = [
        MAGIC_NUMBER.as_slice(),
        &[0u8; 2],
        &ack.process_rank.to_be_bytes(),
        &msg_type.to_be_bytes(),
        ack.msg_ident.as_bytes()
    ].concat();

    let mut mac = HmacSha256::new_from_slice(hmac_key).unwrap();
    mac.update(msg.as_slice());
    let tag = mac.finalize().into_bytes();

    msg = [msg, tag.to_vec()].concat();
    writer.write(msg.as_slice()).await?;

    Ok(())
}