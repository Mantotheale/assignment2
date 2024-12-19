use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;
use crate::{Broadcast, RegisterClient, SystemCallbackType, SystemRegisterCommand};
use crate::stubborn_register_client::stubborn_link::StubbornLink;

mod stubborn_link;
mod timer;

pub struct StubbornRegisterClient {
    links: HashMap<u8, StubbornLink>,
    self_channel: UnboundedSender<(SystemRegisterCommand, SystemCallbackType)>,
    rank: u8,
    processes_count: u8
}

#[async_trait::async_trait]
impl RegisterClient for StubbornRegisterClient {
    async fn send(&self, msg: crate::Send) {
        if self.rank == msg.target {
            let callback: SystemCallbackType = Box::new(|| Box::pin(async move { }));
            self.self_channel.send((msg.cmd.deref().clone(), callback)).unwrap();
        } else {
            let link = self.links.get(&msg.target).unwrap();
            link.add_msg(msg.cmd).await;
        }
    }

    async fn broadcast(&self, msg: Broadcast) {
        for target in 1..self.processes_count + 1 {
            self.send(crate::Send { cmd: msg.cmd.clone(), target}).await;
        }
    }
}

impl StubbornRegisterClient {
    pub fn build(locations: Vec<(String, u16)>,
                 key: Arc<[u8; 64]>,
                 rank: u8,
                 self_channel: UnboundedSender<(SystemRegisterCommand, SystemCallbackType)>) -> Self {
        let mut links = HashMap::new();

        for target in 1..locations.len() + 1 {
            let target = target as u8;

            if target != rank {
                links.insert(target, StubbornLink::build(target, locations.clone(), key.clone()));
            }
        }

        Self {
            links,
            self_channel,
            rank,
            processes_count: locations.len() as u8
        }
    }
}