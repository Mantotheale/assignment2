use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;
use crate::{RegisterClient, Send, Broadcast, SystemRegisterCommand};
use crate::register_client::link_handler::LinkHandler;

pub struct StubbornRegisterClient {
    links: HashMap<u8, UnboundedSender<Arc<SystemRegisterCommand>>>
}

#[async_trait::async_trait]
impl RegisterClient for StubbornRegisterClient {
    async fn send(&self, msg: Send) {
        self.links.get(&msg.target).unwrap().send(msg.cmd).unwrap();
    }

    async fn broadcast(&self, msg: Broadcast) {
        for target in 0..self.links.len() {
            self.send(Send {
                cmd: msg.cmd.clone(),
                target: target as u8,
            }).await;
        }
    }
}

impl StubbornRegisterClient {
    pub async fn build(tcp_locations: Vec<(String, u16)>, hmac_key: Arc<[u8; 64]>) -> StubbornRegisterClient {
        let mut links = HashMap::new();

        let mut i = 0;
        for (address, port) in tcp_locations.iter() {
            links.insert(i + 1, LinkHandler::build(address.clone(), port.clone(), hmac_key.clone()).await);

            i += 1;
        }

        StubbornRegisterClient {
            links
        }
    }
}