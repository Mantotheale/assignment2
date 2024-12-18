use crate::{ClientRegisterCommand, OperationSuccess, RegisterClient, SectorIdx, SectorsManager, SystemRegisterCommand};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use custom_register::CustomAtomicRegister;

mod custom_register;

#[async_trait::async_trait]
pub trait AtomicRegister: Send + Sync {
    /// Handle a client command. After the command is completed, we expect
    /// callback to be called. Note that completion of client command happens after
    /// delivery of multiple system commands to the register, as the algorithm specifies.
    ///
    /// This function corresponds to the handlers of Read and Write events in the
    /// (N,N)-AtomicRegister algorithm.
    async fn client_command(
        &mut self,
        cmd: ClientRegisterCommand,
        success_callback: Box<
            dyn FnOnce(OperationSuccess) -> Pin<Box<dyn Future<Output = ()> + Send>>
            + Send
            + Sync,
        >,
    );

    /// Handle a system command.
    ///
    /// This function corresponds to the handlers of READ_PROC, VALUE, WRITE_PROC
    /// and ACK messages in the (N,N)-AtomicRegister algorithm.
    async fn system_command(&mut self, cmd: SystemRegisterCommand);
}

/// Idents are numbered starting at 1 (up to the number of processes in the system).
/// Communication with other processes of the system is to be done by register_client.
/// And sectors must be stored in the sectors_manager instance.
///
/// This function corresponds to the handlers of Init and Recovery events in the
/// (N,N)-AtomicRegister algorithm.
pub async fn build_atomic_register(
    self_ident: u8,
    sector_idx: SectorIdx,
    register_client: Arc<dyn RegisterClient>,
    sectors_manager: Arc<dyn SectorsManager>,
    processes_count: u8
) -> Box<dyn AtomicRegister> {
    CustomAtomicRegister::build(
        self_ident, sector_idx, register_client, sectors_manager, processes_count
    ).await
}