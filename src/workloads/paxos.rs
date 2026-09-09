#![allow(dead_code)] // FIXME

mod log;
mod paxos_slot;
mod transaction;

use crate::lin_kv::LinKvState;
use crate::messages::{
    LinKvOp, Message, MessageBody, PaxosSeqN, Proposal, RequestBody, ResponseBody,
};
use crate::node_ctx::NodeCtx;
use crate::workloads::paxos::log::Log;
use crate::workloads::paxos::paxos_slot::PaxosSlot;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use tokio::sync::{OwnedRwLockWriteGuard, RwLock};
use tracing::info;

/// Represents a node in a cluster of hosts running paxos. Encapsulates all that logic & state.
pub struct PaxosNode {
    // I think I gave up prematurely on not having this in an Arc. Might be possible to just have a NodeCtx here...
    node_ctx: Arc<NodeCtx>,
    /// The node id of the current leader. See [`NodeCtx::nodes`]
    cur_leader: RwLock<String>,
    /// The current sequence number for this paxos node. All paxos rounds/slots use this same counter
    paxos_seq_num: AtomicUsize,
    /// The titular key-value store. Once operations are decided in the log, they are applied (in order)
    /// to this snapshot.
    snapshot: RwLock<LinKvState>,
    /// Index of the next slot in the log to apply to the snapshot (once it's ready). May be equal
    /// to `log.len()` if all slots in the log have been applied already
    next_slot_to_apply: AtomicUsize,
    /// Log of the paxos slots. Each slot represents one round of paxos, corresponding to one atomic
    /// operation on the snapshot
    // log: Vec<RwLock<PaxosSlot>>,
    log: Arc<Log>,
}

/// Result of phase 1 of the paxos algorithm for one slot.
enum Phase1Result {
    /// We successfully prepared a quorum of acceptors, and didn't discover an already accepted proposal.
    /// We're ready to try proposing a value of our choosing using this sequence number
    Success { slot_idx: usize, seq_num: PaxosSeqN },
    /// We contacted a quorum of acceptors, and learned that there's already been an accepted proposal.
    /// We're now duty bound to adopt that value for our proposal, and we'll need to use a new slot
    /// for our desired value
    AlreadyAccepted(Proposal),
    /// We tried preparing the slot, but turns out it's already been fully decided (we just didn't
    /// hear about it until now). We need to update our snapshot, and try our proposal again in
    /// a new slot
    AlreadyDecided(LinKvOp),
    /// We failed to contact a quorum of nodes
    TimedOut,
}

enum Phase2Result {
    /// We successfully proposed the value to a majority of approvers! Next step is to mark the
    /// slot decided, and share the news!
    Success { value: LinKvOp },
    /// After our prepare (but before our proposal) someone else swept in and prepared
    /// a new sequence number above ours, and our proposal was rejected! We need to try again with a
    /// new sequence number. The value is the new highest sequence number (the one we lost to)
    NTooLow(PaxosSeqN),
    /// We tried proposing a value for this spot, but it turns out it's already been fully decided.
    AlreadyDecided(LinKvOp),
    /// We weren't able to contact a quorum of nodes
    TimedOut,
}

// ===== Misc utility functions =====
impl PaxosNode {
    /// Checks if I'm leader based on the current in-memory state. Doesn't do health checks to
    /// make sure that the rest of the cluster agrees I'm leader
    async fn i_think_im_leader(&self) -> bool {
        let cur_leader = self.cur_leader.read().await;
        cur_leader.as_ref() == self.node_ctx.node_id
    }

    fn min_quorum_size(&self) -> usize {
        self.node_ctx.min_quorum_size()
    }

    async fn get_slot_mut(&self, slot_idx: usize) -> Option<OwnedRwLockWriteGuard<PaxosSlot>> {
        let locked = self.log.get(slot_idx)?;
        Some(locked.write_owned().await)
    }

    // fn next_seq_num(&self, slot: &PaxosSlot) -> PaxosSeqN {
    //     let node_id = self.node_ctx.node_id.clone();
    //     match slot {
    //         PaxosSlot::New(..) => PaxosSeqN {
    //             counter: 0,
    //             node_id,
    //         },
    //         PaxosSlot::Promised { promised_seq_num }
    //         | PaxosSlot::Accepted {
    //             promised_seq_num, ..
    //         } => PaxosSeqN {
    //             counter: promised_seq_num.counter + 1,
    //             node_id,
    //         },
    //         PaxosSlot::Decided { .. } => {
    //             panic!(
    //                 "Tried to generate the next seq number for already decided slot. There is no next seq number, paxos is all done here!"
    //             );
    //         }
    //     }
    // }
}

// ===== Message handling/overall plumbing =====
impl PaxosNode {
    pub fn new(node_ctx: NodeCtx) -> Self {
        let cur_leader = node_ctx
            .nodes
            .iter()
            .min()
            .cloned()
            .expect("NodeCtx has no nodes! It's gonna be hard to do paxos without them!");

        info!("Assuming {cur_leader} is leader node (TODO: Do this through an election)");

        Self {
            node_ctx: Arc::new(node_ctx),
            cur_leader: RwLock::new(cur_leader),
            paxos_seq_num: Default::default(),
            snapshot: Default::default(),
            next_slot_to_apply: Default::default(),
            log: Arc::new(Log::new()),
        }
    }

    pub async fn run(self: Arc<Self>) -> anyhow::Result<()> {
        while let Some(message) = self.node_ctx.next_request().await {
            let self_arc = Arc::clone(&self);
            tokio::spawn(async move {
                let resp = self_arc.handle_request(&message).await;
                self_arc.node_ctx.send_response(&message, resp).await;
            });
        }

        Ok(())
    }

    async fn handle_request(&self, message: &Message) -> ResponseBody {
        let Message {
            body:
                MessageBody::Request {
                    msg_id: _,
                    req_body: _,
                },
            ..
        } = message
        else {
            panic!(
                "This function is only supposed to handle requests! Got: {:?}",
                message
            );
        };

        todo!()
        // match req_body {
        //     RequestBody::Init(_) | RequestBody::Echo { .. } => ResponseBody::Error {
        //         code: ErrorCode::NotSupported,
        //         text: Some(format!("paxos doesn't support request: {:?}", message)),
        //     },
        //     RequestBody::LinKvOp(op) => self.handle_linkv_op(op).await,
        // }
    }
}

// ===== Actual Paxos implementation =====

impl PaxosNode {
    async fn handle_linkv_op(&self, op: &LinKvOp) -> ResponseBody {
        // If there's only 1 node, we can just make the change to our snapshot. Ezpz!
        if self.node_ctx.nodes.len() == 1 {
            let mut snapshot = self.snapshot.write().await;
            return snapshot.apply(op.clone());
        }

        if !self.i_think_im_leader().await {
            // Just proxy the request to the leader & let them take care of it
            return self
                .node_ctx
                .send_request(
                    self.cur_leader.read().await.clone(),
                    RequestBody::LinKvOp(op.clone()),
                )
                .await
                .expect("TODO"); // FIXME
        }

        todo!()
    }
}
