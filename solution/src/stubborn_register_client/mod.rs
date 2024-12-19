use std::sync::Arc;
use crate::{Broadcast, RegisterClient};
use crate::stubborn_register_client::stubborn_link::StubbornLink;

mod stubborn_link;
mod timer;

pub struct StubbornRegisterClient {
    links: Vec<StubbornLink>
}

#[async_trait::async_trait]
impl RegisterClient for StubbornRegisterClient {
    async fn send(&self, msg: crate::Send) {
        let link = self.links.get(msg.target as usize - 1).unwrap();
        link.add_msg(msg.cmd).await;
    }

    async fn broadcast(&self, msg: Broadcast) {
        for target in 1..self.links.len() + 1 {
            self.send(crate::Send { cmd: msg.cmd.clone(), target: target as u8 }).await;
        }
    }
}

impl StubbornRegisterClient {
    pub fn build(locations: Vec<(String, u16)>, key: Arc<[u8; 64]>) -> Self {
        let mut links = Vec::new();

        for target in 1..locations.len() + 1 {
            links.push(StubbornLink::build(target as u8, locations.clone(), key.clone()));
        }

        Self {
            links
        }
    }
}