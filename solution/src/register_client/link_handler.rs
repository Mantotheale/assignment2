use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::{mpsc, Mutex};
use uuid::Uuid;
use crate::register_client::timer_handle::TimerHandle;
use crate::{RegisterCommand, serialize_register_command, SystemRegisterCommand, SystemRegisterCommandContent};
use crate::transfer::transfer_ack::deserialize_ack;

#[derive(Clone)]
pub struct LinkHandler {
    pending_acknowledgments: Arc<Mutex<HashMap<Acknowledgment, TimerHandle>>>,
    sender: UnboundedSender<Arc<SystemRegisterCommand>>
}

impl LinkHandler {
    pub async fn build(address: String, port: u16, key: Arc<[u8; 64]>) -> UnboundedSender<Arc<SystemRegisterCommand>> {
        let stream = TcpStream::connect((address.as_str(), port)).await.unwrap();
        let (read_stream, write_stream) = stream.into_split();

        let (add_msg_tx, add_msg_rx) = mpsc::unbounded_channel();
        let (send_msg_tx, send_msg_rx) = mpsc::unbounded_channel();

        let handler = LinkHandler {
            pending_acknowledgments: Arc::new(Mutex::new(HashMap::new())),
            sender: send_msg_tx
        };

        tokio::spawn(add_message_background(handler.clone(), add_msg_rx));
        tokio::spawn(send_messages_background(write_stream, key.clone(), send_msg_rx));
        tokio::spawn(acknowledgments_background(read_stream, key, handler));

        add_msg_tx
    }

    async fn ack_received(&mut self, ack: Acknowledgment) {
        let timer = self.pending_acknowledgments.lock().await.remove(&ack);

        if let Some(t) = timer { t.stop().await; }
    }

    pub async fn add_msg(&mut self, msg: Arc<SystemRegisterCommand>) {
        let ack = Acknowledgment {
            process_rank: msg.header.process_identifier,
            msg_ident: msg.header.msg_ident,
            msg_type: match msg.content {
                SystemRegisterCommandContent::ReadProc => MessageType::ReadProc,
                SystemRegisterCommandContent::Value { .. } => MessageType::Value,
                SystemRegisterCommandContent::WriteProc { .. } => MessageType::WriteProc,
                SystemRegisterCommandContent::Ack => MessageType::Ack,
            }
        };

        self.pending_acknowledgments.lock().await.insert(ack, TimerHandle::start_timer(msg, self.sender.clone()));
    }
}

async fn add_message_background(mut handler: LinkHandler, mut msg_queue: UnboundedReceiver<Arc<SystemRegisterCommand>>) {
    loop {
        let command = msg_queue.recv().await;

        if let None = command {
            break;
        }

        let command = command.unwrap();

        handler.add_msg(command).await;
    }
}

async fn send_messages_background(mut stream: OwnedWriteHalf, key: Arc<[u8; 64]>, mut msg_queue: UnboundedReceiver<Arc<SystemRegisterCommand>>) {
    loop {
        let command = msg_queue.recv().await;

        if let None = command {
            break;
        }

        let command = command.unwrap();
        serialize_register_command(&RegisterCommand::System(command.deref().clone()), &mut stream, key.deref()).await.unwrap();
    }
}

async fn acknowledgments_background(mut stream: OwnedReadHalf, key: Arc<[u8; 64]>, mut handler: LinkHandler) {
    loop {
        let ack = wait_next_acknowledgment(&mut stream, key.clone()).await;

        handler.ack_received(ack).await;
    }
}

async fn wait_next_acknowledgment(stream: &mut OwnedReadHalf, key: Arc<[u8; 64]>) -> Acknowledgment {
    loop {
        let result = deserialize_ack(stream, key.deref()).await;

        if let Ok((ack, true)) = result {
            return  ack;
        }
    };
}

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