pub use domain::*;
pub use atomic_register_public::*;
pub use register_client_public::*;
pub use sectors_manager_public::*;
pub use transfer_public::*;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use hmac::Hmac;
use sha2::Sha256;
use tokio::net::TcpListener;
use tokio::sync::mpsc::unbounded_channel;
use crate::stubborn_register_client::StubbornRegisterClient;
use crate::registers_dispatcher::RegistersDispatcher;
use crate::stream_handler::StreamHandler;

mod domain;
mod transfer_public;
mod atomic_register_public;
mod sectors_manager_public;
mod register_client_public;

mod stubborn_register_client;
mod transfer;
mod registers_dispatcher;
mod tcp_writer;
mod stream_handler;

type HmacSha256 = Hmac<Sha256>;
type SuccessCallbackType = Box<dyn FnOnce(OperationSuccess) -> Pin<Box<dyn Future<Output=()> + std::marker::Send>> + std::marker::Send + Sync>;

pub async fn run_register_process(config: Configuration) {
    let system_key = Arc::new(config.hmac_system_key);
    let client_key = Arc::new(config.hmac_client_key);
    let storage_dir = config.public.storage_dir;
    let tcp_locations = config.public.tcp_locations;
    let rank = config.public.self_rank;
    let num_processes = tcp_locations.len() as u8;
    let n_sectors = config.public.n_sectors;

    let (system_tx, system_rx) = unbounded_channel();

    let sectors_manager = build_sectors_manager(storage_dir).await;
    let register_client = Arc::new(StubbornRegisterClient::build(
        tcp_locations.clone(), system_key.clone(), rank, system_tx.clone()
    ));

    let dispatcher = RegistersDispatcher::build(
        system_tx,
        system_rx,
        rank,
        register_client,
        sectors_manager,
        num_processes
    );

    let location = tcp_locations.get((rank - 1) as usize).unwrap();
    let listener = TcpListener::bind((location.0.as_str(), location.1)).await.unwrap();

    loop {
        let (stream, _) = listener.accept().await.unwrap();

        let mut stream_handler = StreamHandler::build(
            stream, system_key.clone(), client_key.clone(), n_sectors, dispatcher.clone()
        );

        tokio::spawn(async move { stream_handler.run().await; });
    }
}