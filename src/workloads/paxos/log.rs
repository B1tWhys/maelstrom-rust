use crate::lin_kv::LinKvState;
use crate::workloads::paxos::paxos_slot::PaxosSlot;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

#[derive(Debug, Default)]
pub struct Log {
    slots: RwLock<Vec<Arc<RwLock<PaxosSlot>>>>,
    snapshot: RwLock<LinKvState>,
    next_slot_to_commit: AtomicUsize,
    // This is kinda bad. This should have just been an AtomicBoolean on the PaxosSlot itself, but
    // that's a hassle to do now coz I shortsightedly made it an enum. Good enough for now...
    claimed_slots: RwLock<HashSet<usize>>,
}

impl Log {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn claim_slot(self: &Arc<Log>) -> ClaimedSlot {
        // This is incredibad, I'm just brute forcing this by spraying locks everywhere. I really
        // fear deadlocks... hopefully it's good enough for now tho
        let mut slots = self.slots.write().await;
        let mut claimed = self.claimed_slots.write().await;
        let mut i = self.next_slot_to_commit.load(Ordering::Relaxed);

        while i < slots.len() {
            let slot = slots.get(i).unwrap();
            match &*slot.read().await {
                PaxosSlot::New { .. } | PaxosSlot::Promised { .. } => {
                    claimed.insert(i);
                    return ClaimedSlot(slot.clone(), i, self.clone());
                }
                PaxosSlot::Accepted { .. } | PaxosSlot::Decided { .. } => {
                    i += 1;
                }
            }
        }

        let slot = Arc::new(RwLock::new(PaxosSlot::New {
            slot_idx: i,
            last_promised_seq_num: None,
        }));
        let claim = ClaimedSlot(slot.clone(), i, self.clone());
        claimed.insert(i);
        slots.push(slot);
        claim
    }

    pub async fn get(&self, i: usize) -> Option<Arc<RwLock<PaxosSlot>>> {
        self.slots.read().await.get(i).cloned()
    }
}

#[derive(Debug)]
pub struct ClaimedSlot(Arc<RwLock<PaxosSlot>>, usize, Arc<Log>);

impl ClaimedSlot {
    pub async fn read(&self) -> RwLockReadGuard<'_, PaxosSlot> {
        self.0.read().await
    }

    pub async fn write(&self) -> RwLockWriteGuard<'_, PaxosSlot> {
        self.0.write().await
    }
}

impl Drop for ClaimedSlot {
    fn drop(&mut self) {
        self.2.claimed_slots.blocking_write().remove(&self.1);
    }
}
