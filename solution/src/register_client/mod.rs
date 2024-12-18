use std::ops::Deref;
use std::sync::Arc;
use tokio::net::TcpStream;
use crate::{Broadcast, RegisterClient, RegisterCommand, serialize_register_command};

pub struct SimpleRegisterClient {
    locations: Vec<(String, u16)>,
    key: Arc<[u8; 64]>
}

#[async_trait::async_trait]
impl RegisterClient for SimpleRegisterClient {
    async fn send(&self, msg: crate::Send) {
        let mut stream = self.connect(msg.target).await;
        serialize_register_command(&RegisterCommand::System(msg.cmd.deref().clone()), &mut stream, self.key.deref()).await.unwrap();
    }

    async fn broadcast(&self, msg: Broadcast) {
        for target in 1..self.locations.len() + 1 {
            self.send(crate::Send { cmd: msg.cmd.clone(), target: target as u8 }).await;
        }
    }
}

impl SimpleRegisterClient {
    pub fn build(locations: Vec<(String, u16)>, key: Arc<[u8; 64]>) -> Self {
        Self {
            locations,
            key
        }
    }


    async fn connect(&self, target: u8) -> TcpStream {
        let location = self.locations.get((target - 1) as usize).unwrap();
        TcpStream::connect((location.0.as_str(), location.1)).await.unwrap()
    }
}