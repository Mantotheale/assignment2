use std::ops::Deref;
use std::sync::Arc;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use crate::transfer::{Acknowledgment, RegisterResponse, serialize_ack, serialize_register_response};

#[derive(Clone)]
pub struct TcpWriter {
    response_tx: UnboundedSender<RegisterResponse>,
    ack_tx: UnboundedSender<Acknowledgment>
}

impl TcpWriter {
    pub fn build(stream: OwnedWriteHalf, system_key: Arc<[u8; 64]>, client_key: Arc<[u8; 32]>) -> Self{
        let (response_tx, response_rx) = unbounded_channel();
        let (ack_tx, ack_rx) = unbounded_channel();

        tokio::spawn(Self::background(
            response_rx, ack_rx, stream, client_key, system_key)
        );

        Self { response_tx, ack_tx }
    }

    pub fn send_response(&self, response: RegisterResponse) {
        _ = self.response_tx.send(response);
    }

    pub fn send_ack(&self, ack: Acknowledgment) {
        _ = self.ack_tx.send(ack);
    }

    async fn background(mut response_tx: UnboundedReceiver<RegisterResponse>,
                        mut ack_rx: UnboundedReceiver<Acknowledgment>,
                        mut stream: OwnedWriteHalf,
                        client_key: Arc<[u8; 32]>,
                        system_key: Arc<[u8; 64]>) {
        loop {
            tokio::select! {
                Some(response) = response_tx.recv() => {
                    _ = serialize_register_response(&response, &mut stream, client_key.clone().deref()).await;
                }
                Some(ack) = ack_rx.recv() => {
                    _ = serialize_ack(&ack, &mut stream, system_key.clone().deref()).await;
                }
                else => break
            }
        }
    }
}