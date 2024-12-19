use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Sender, UnboundedSender};
use tokio::time;
use crate::SystemRegisterCommand;

struct StopTimer { }

pub struct TimerHandle {
    stop_tx: Sender<StopTimer>,
}

impl TimerHandle {
    pub async fn stop(self) {
        self.stop_tx.try_send(StopTimer {}).unwrap();
    }

    pub fn start_timer(message: Arc<SystemRegisterCommand>, sender: UnboundedSender<Arc<SystemRegisterCommand>>) -> TimerHandle {
        let (stop_tx, mut stop_rx) = mpsc::channel(1);

        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_millis(250));
            interval.tick().await;

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