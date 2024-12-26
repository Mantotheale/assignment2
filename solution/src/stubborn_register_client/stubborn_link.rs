use std::collections::HashMap;
use std::ops::Deref;
use std::sync::{Arc};
use std::time::Duration;
use tokio::sync::{mpsc, Mutex};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{Sender, unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::time;
use crate::{RegisterCommand, serialize_register_command, SystemRegisterCommand};
use crate::transfer::{Acknowledgment, deserialize_ack};

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

        let link = Self {
            pending_acks: Arc::new(Mutex::new(HashMap::new())),
            msg_tx,
            target_rank,
        };

        tokio::spawn(link_background(link.clone(), msg_rx, address.clone(), *port, key));

        link
    }

    pub async fn add_msg(&self, msg: Arc<SystemRegisterCommand>) {
        let ack = Acknowledgment::from_cmd(msg.deref().clone(), self.target_rank);
        
        let timer = TimerHandle::start_timer(msg, self.msg_tx.clone(), Duration::from_millis(250));
        self.pending_acks.lock().await.insert(ack, timer);
    }

    async fn ack_received(&self, ack: Acknowledgment) {
        if let Some(timer_handle) = self.pending_acks.lock().await.remove(&ack) {
            timer_handle.stop().await;
        }
    }
}

async fn link_background(link: StubbornLink,
                         msg_queue: UnboundedReceiver<Arc<SystemRegisterCommand>>,
                         address: String,
                         port: u16,
                         key: Arc<[u8; 64]>) {
    let msg_sender = MsgSender::build(msg_queue, key.clone());

    loop {
        let Ok(stream) = TcpStream::connect((address.as_str(), port)).await else { continue; };
        let (read_stream, write_stream) = stream.into_split();

        msg_sender.change_stream(write_stream);

        let mut ack_listener = AckListener { link: link.clone(), stream: read_stream, key: key.clone() };
        ack_listener.listen_acks().await;
    }
}

struct MsgSender {
    stream_tx: UnboundedSender<OwnedWriteHalf>
}

impl MsgSender {
    fn build(msg_queue: UnboundedReceiver<Arc<SystemRegisterCommand>>,
             key: Arc<[u8; 64]>) -> Self {
        let (stream_tx, stream_rx) = unbounded_channel();

        tokio::spawn(Self::background(msg_queue, stream_rx, key));

        Self { stream_tx }
    }

    fn change_stream(&self, stream: OwnedWriteHalf) {
        _ = self.stream_tx.send(stream)
    }

    async fn background(mut msg_queue: UnboundedReceiver<Arc<SystemRegisterCommand>>,
                  mut stream_rx: UnboundedReceiver<OwnedWriteHalf>,
                  key: Arc<[u8; 64]>) {
        let Some(mut write_stream) = stream_rx.recv().await else { return; };

        loop {
            tokio::select! {
                biased;
                Some(result) = stream_rx.recv() => write_stream = result,
                Some(msg) = msg_queue.recv() => {
                     _ = serialize_register_command(
                        &RegisterCommand::System(msg.deref().clone()),
                        &mut write_stream,
                        key.clone().deref()
                    ).await;
                },
                else => break
            }
        }
    }
}

struct AckListener {
    link: StubbornLink,
    stream: OwnedReadHalf,
    key: Arc<[u8; 64]>
}

impl AckListener {
    async fn listen_acks(&mut self) {
        loop {
            let Ok(ack) = self.wait_next_acknowledgment().await else { break; };
            self.link.ack_received(ack).await;
        }
    }

    async fn wait_next_acknowledgment(&mut self) -> Result<Acknowledgment, ()> {
        loop {
            let mut buf = [0u8; 1];
            let Ok(bytes_read) = self.stream.peek(&mut buf).await else { return Err(()) };
            if bytes_read == 0 { return Err(()) }

            let result = deserialize_ack(&mut self.stream, self.key.deref()).await;

            if let Ok((ack, true)) = result {
                return Ok(ack)
            }
        };
    }
}

struct StopTimer { }

struct TimerHandle {
    stop_tx: Sender<StopTimer>,
}

impl TimerHandle {
    async fn stop(self) {
        _ = self.stop_tx.send(StopTimer {});
    }

    fn start_timer(message: Arc<SystemRegisterCommand>,
                   sender: UnboundedSender<Arc<SystemRegisterCommand>>,
                   duration: Duration) -> TimerHandle {
        let (stop_tx, mut stop_rx) = mpsc::channel(1);

        tokio::spawn(async move {
            let mut interval = time::interval(duration);

            loop {
                tokio::select! {
                    biased;
                    _ = stop_rx.recv() => break,
                    _ = interval.tick() => sender.send(message.clone()).unwrap()
                }
            }
        });

        TimerHandle { stop_tx }
    }
}