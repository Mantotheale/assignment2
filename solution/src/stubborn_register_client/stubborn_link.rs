use std::ops::Deref;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use crate::{RegisterCommand, serialize_register_command, SystemRegisterCommand};

pub struct StubbornLink {
    msg_tx: UnboundedSender<Arc<SystemRegisterCommand>>
}

impl StubbornLink {
    pub fn build(address: String, port: u16, key: Arc<[u8; 64]>) -> Self {
        let (msg_tx, msg_rx) = unbounded_channel();

        tokio::spawn(listen_msg_request(
           msg_rx, address, port, key
        ));
        Self {
            msg_tx
        }
    }

    pub fn send_msg(&self, msg: Arc<SystemRegisterCommand>) {
        self.msg_tx.send(msg).unwrap();
    }
}

async fn listen_msg_request(mut msg_queue: UnboundedReceiver<Arc<SystemRegisterCommand>>,
                            address: String,
                            port: u16,
                            key: Arc<[u8; 64]>) {
    loop {
        let stream = TcpStream::connect((address.as_str(), port)).await;

        if let Ok(mut stream) = stream {
            loop {
                let msg = msg_queue.recv().await;
                if msg.is_none() { return; }
                let msg = msg.unwrap();

                serialize_register_command(
                    &RegisterCommand::System(msg.deref().clone()),
                    &mut stream,
                    key.deref()
                ).await.unwrap();
            }
        }
    }
}