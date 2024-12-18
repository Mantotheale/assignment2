use std::io::Error;
use hmac::Mac;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use crate::{MAGIC_NUMBER, OperationReturn, OperationSuccess, StatusCode};
use crate::HmacSha256;

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