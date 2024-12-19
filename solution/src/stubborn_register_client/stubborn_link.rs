use std::collections::HashMap;
use std::ops::Deref;
use std::sync::{Arc};
use tokio::sync::Mutex;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use crate::{RegisterCommand, serialize_register_command, SystemRegisterCommand, SystemRegisterCommandContent};
use crate::stubborn_register_client::timer::TimerHandle;
use crate::transfer::{Acknowledgment, deserialize_ack, MessageType};

#[derive(Clone)]
pub struct StubbornLink {
    pending_acks: Arc<Mutex<HashMap<Acknowledgment, TimerHandle>>>,
    msg_tx: UnboundedSender<Arc<SystemRegisterCommand>>,
    target_rank: u8,
}

impl StubbornLink {
    pub fn build(target_rank: u8, locations: Vec<(String, u16)>, key: Arc<[u8; 64]>) -> Self {
        let (address, port) = locations.get((target_rank - 1) as usize).unwrap();
        let (msg_tx, msg_rx) = unbounded_channel();

        let handler = StubbornLink {
            pending_acks: Arc::new(Mutex::new(HashMap::new())),
            msg_tx,
            target_rank,
        };

        tokio::spawn(link_background(
            handler.clone(), msg_rx, address.clone(), *port, key
        ));

        handler
    }

    pub async fn add_msg(&self, msg: Arc<SystemRegisterCommand>) {
        let ack = Acknowledgment {
            msg_type: match msg.content {
                SystemRegisterCommandContent::ReadProc => MessageType::ReadProc,
                SystemRegisterCommandContent::Value { .. } => MessageType::Value,
                SystemRegisterCommandContent::WriteProc { .. } => MessageType::WriteProc,
                SystemRegisterCommandContent::Ack => MessageType::Ack
            },
            process_rank: self.target_rank,
            msg_ident: msg.header.msg_ident,
        };
        
        let timer = TimerHandle::start_timer(msg, self.msg_tx.clone());
        self.pending_acks.lock().await.insert(ack, timer);
    }

    async fn ack_received(&self, ack: Acknowledgment) {
        if let Some(timer_handle) = self.pending_acks.lock().await.remove(&ack) {
            timer_handle.stop().await;
        }
    }
}

async fn link_background(handler: StubbornLink,
                         msg_queue: UnboundedReceiver<Arc<SystemRegisterCommand>>,
                         address: String,
                         port: u16,
                         key: Arc<[u8; 64]>) {
    let (stream_tx, stream_rx) = unbounded_channel();

    tokio::spawn(send_messages(msg_queue, stream_rx, key.clone()));

    loop {
        let result = TcpStream::connect((address.as_str(), port)).await;
        if result.is_err() { continue; }
        let stream = result.unwrap();
        let (read_stream, write_stream) = stream.into_split();

        stream_tx.send(write_stream).unwrap();

        listen_acks(handler.clone(), read_stream, key.clone()).await;
    }
}

async fn send_messages(mut msg_queue: UnboundedReceiver<Arc<SystemRegisterCommand>>,
                       mut stream_rx: UnboundedReceiver<OwnedWriteHalf>,
                       key: Arc<[u8; 64]>) {
    let result = stream_rx.recv().await;
    if result.is_none() { return; }
    let mut write_stream = result.unwrap();

    loop {
        tokio::select! {
            biased;
            result = stream_rx.recv() => {
                if result.is_none() { break; }
                write_stream = result.unwrap();
            },
            result = msg_queue.recv() => {
                if result.is_none() { break; }
                let msg = result.unwrap();

                let  _ = serialize_register_command(&RegisterCommand::System(msg.deref().clone()), &mut write_stream, key.clone().deref()).await;
            }
        }
    }
}

async fn listen_acks(handler: StubbornLink, mut read_stream: OwnedReadHalf, key: Arc<[u8; 64]>) {
    loop {
        let result = wait_next_acknowledgment(&mut read_stream, key.clone()).await;

        if result.is_err() {
            break;
        }

        let ack = result.unwrap();
        handler.ack_received(ack).await;
    }
}

async fn wait_next_acknowledgment(stream: &mut OwnedReadHalf, key: Arc<[u8; 64]>) -> Result<Acknowledgment, ()> {
    loop {
        let mut buf = [0u8; 1];
        let result = stream.peek(&mut buf).await;

        if result.is_err() {
            return Err(());
        } else if let Ok(0) = result {
            return Err(());
        }

        let result = deserialize_ack(stream, key.deref()).await;

        if let Ok((ack, true)) = result {
            return Ok(ack)
        }
    };
}