use crate::workloads::paxos::paxos_slot::PaxosSlot;
use std::sync::Arc;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

#[derive(Debug)]
pub struct Log {}

impl Log {
    pub fn new() -> Self {
        todo!()
    }
    pub async fn claim_slot(&self) -> ClaimedSlot {
        todo!()
    }
    pub fn get(&self, _i: usize) -> Option<Arc<RwLock<PaxosSlot>>> {
        todo!()
    }
}

#[derive(Debug)]
pub struct ClaimedSlot(Arc<RwLock<PaxosSlot>>, Arc<Log>);

impl ClaimedSlot {
    pub async fn read(&self) -> RwLockReadGuard<'_, PaxosSlot> {
        self.0.read().await
    }

    pub async fn write(&self) -> RwLockWriteGuard<'_, PaxosSlot> {
        self.0.write().await
    }
}
