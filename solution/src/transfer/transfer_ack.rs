use std::collections::VecDeque;
use crate::{HmacSha256, MAGIC_NUMBER};
use std::io::Error;
use std::io::ErrorKind::InvalidInput;
use hmac::Mac;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
pub struct Acknowledgment {
    pub msg_type: MessageType,
    pub process_rank: u8,
    pub msg_ident: Uuid,
}

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
pub enum MessageType {
    ReadProc,
    Value,
    WriteProc,
    Ack
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

    let msg_ident = data.read_u128().await?;

    let mut hmac_tag = [0u8; 32];
    data.read_exact(&mut hmac_tag).await?;

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
    let is_valid = mac.verify_slice(hmac_tag.as_slice()).is_ok();

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