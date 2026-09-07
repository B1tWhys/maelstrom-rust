#![allow(dead_code)] // FIXME

use crate::common::NodeCtx;
use crate::countdown_latch::CountDownLatch;
use crate::lin_kv::LinKvState;
use crate::messages::{
    LinKvOp, Message, MessageBody, PaxosNack, PaxosReq, PaxosRsp, PaxosSeqN, Proposal, RequestBody,
    ResponseBody,
};
use anyhow::bail;
use std::fmt::Debug;
use std::hash::Hash;
use std::ops::DerefMut;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::{RwLock, RwLockMappedWriteGuard, RwLockWriteGuard};
use tokio::task::JoinSet;
use tracing::{debug, error, info, warn};

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
    log: Vec<RwLock<PaxosSlot>>,
}

/// State machine for 1 round of paxos
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
enum PaxosSlot {
    Gap, // TODO: add seq num here too, so we don't forget it if we're reusing this slot
    Promised {
        promised_seq_num: PaxosSeqN,
    },
    Accepted {
        promised_seq_num: PaxosSeqN,
        accepted_seq_num: PaxosSeqN,
        value: LinKvOp,
    },
    Decided {
        operation: LinKvOp,
        result: ResponseBody
    },
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
        let l = self.node_ctx.nodes.len();
        l / 2 + 1
    }

    async fn get_slot_mut(&self, slot_idx: usize) -> Option<RwLockWriteGuard<PaxosSlot>> {
        if let Some(fut) = self.log.get(slot_idx).map(|s| s.write()) {
            return Some(fut.await);
        }
        None
    }

    fn next_seq_num(&self, slot: &PaxosSlot) -> PaxosSeqN {
        let node_id = self.node_ctx.node_id.clone();
        match slot {
            PaxosSlot::Gap => PaxosSeqN {
                counter: 0,
                node_id,
            },
            PaxosSlot::Promised { promised_seq_num }
            | PaxosSlot::Accepted {
                promised_seq_num, ..
            } => PaxosSeqN {
                counter: promised_seq_num.counter + 1,
                node_id,
            },
            PaxosSlot::Decided { .. } => {
                panic!(
                    "Tried to generate the next seq number for already decided slot. There is no next seq number, paxos is all done here!"
                );
            }
        }
    }
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
            log: vec![],
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
                req_body,
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

    async fn run_paxos(&self, op: &LinKvOp) -> anyhow::Result<()> { // TODO: Fix return type
        'slot: loop {
            let (slot_idx, mut slot) = self.pick_slot_for_new_transaction().await;
            'seq_num: loop {
                let seq_num = match self.phase1(slot_idx, &mut slot, op).await {
                    Phase1Result::Success { seq_num, .. } => seq_num.clone(),
                    Phase1Result::AlreadyAccepted(prop) => {
                        // This slot already had an accepted proposal, spawn an async task to propose that
                        // value for this slot to the rest of the acceptors, and then select a new slot
                        // and run the whole process over again
                        tokio::spawn(async move {
                            self.phase2(slot_idx, &mut slot, prop).await
                        });

                        continue 'slot;
                    }
                    Phase1Result::AlreadyDecided(decided_op) => {
                        tokio::spawn(async move {
                            slot.decide(decided_op);
                            self.commit().await;
                        });
                        continue 'slot;
                    }
                    Phase1Result::TimedOut => {
                        bail!("TODO: Better error here");
                    }
                };

                let prop = Proposal {
                    seq_num,
                    value: op.clone(),
                };
                match self.phase2(slot_idx, &mut slot, prop).await {
                    Phase2Result::Success { value } => {
                        // tokio::spawn(self.notify_other_learners_decided(&mut slot, value));

                    }
                    Phase2Result::NTooLow(_) => {}
                    Phase2Result::AlreadyDecided(_) => {}
                    Phase2Result::TimedOut => {}
                };
            }
        }

        todo!()
    }

    async fn phase1(&self, slot_idx: usize, slot: &mut RwLockWriteGuard<'_, PaxosSlot>, op: &LinKvOp) -> Phase1Result {
        todo!()
    }

    /*
    async fn phase1_tmp(&self, op: &LinKvOp) -> Phase1Result {
        let slot = self.pick_slot_for_new_transaction().await;
        // let (slot_idx, mut seq_num) = self.prepare_local_slot().await;
        loop {
            debug!(slot_idx, %seq_num, "Prepping slot");

            // Async fire off 'prep requests to all acceptors
            let mut prep_req_tasks = JoinSet::new();
            for dest in self.node_ctx.other_nodes().cloned() {
                let req = RequestBody::Paxos(PaxosReq::Prepare {
                    seq_num: seq_num.clone(),
                    slot_idx,
                });
                let node_ctx = Arc::clone(&self.node_ctx);

                // TODO: timeout
                prep_req_tasks.spawn(async move { node_ctx.send_request(dest, req).await });
            }

            let mut responses = vec![];
            while let Some(resp) = prep_req_tasks.join_next().await {
                let resp = resp.expect(
                    "Not sure why the join can fail... cross that bridge when I come to it",
                );

                match resp {
                    Ok(ResponseBody::Paxos(r)) => {
                        responses.push(r);
                        if matches!(responses.last(), Some(PaxosRsp::Nack(_))) {
                            break;
                        }
                    }
                    Ok(r) => {
                        panic!("Non-paxos response received for prepare request: {r:?}");
                    }
                    Err(e) => {
                        debug!(%e, "Failed to propose to an acceptor. No biggie");
                    }
                }
                // We only need responses from a simple majority of nodes, so we can stop early once
                // we reach that. Note that the current node counts as 1 member/response
                if responses.len() >= self.min_quorum_size() - 1 {
                    break;
                }
            }
            // Cancel any requests that are in-flight still
            drop(prep_req_tasks);

            // If we didn't get the necessary number of responses, give up (presumably there's
            // a network partition or something)
            if responses.len() < self.min_quorum_size() - 1 {
                info!(prep_rsp_count = responses.len(), ?op, "Phase 1 timed out");
                return Phase1Result::TimedOut;
            }

            if let Some(existing_seq_no) = responses
                .iter()
                .filter_map(|r| match r {
                    PaxosRsp::Nack(PaxosNack::NTooLow(seq_num)) => Some(seq_num),
                    _ => None,
                })
                .max()
            {
                // There was already a higher promised sequence number. We need to try again with a
                // higher sequence number
                let mut slot = self
                    .get_slot_mut(slot_idx)
                    .await
                    .expect("This slot was created at the beginning of this function");
                seq_num = PaxosSeqN {
                    counter: existing_seq_no.counter + 1,
                    node_id: seq_num.node_id,
                };

                slot.try_prepare(seq_num.clone());
            }

            let mut result = Phase1Result::Success { slot_idx, seq_num };
            for resp in responses {
                match resp {
                    PaxosRsp::Promise { .. } => {}
                    PaxosRsp::AcceptOk => {
                        panic!("This wasn't an accept request")
                    }
                    PaxosRsp::Nack(PaxosNack::NTooLow()) => {}
                }
            }

            todo!()
        }
    }
     */

    /// Atomically find (or create) an empty slot in the log, increment our paxos_seq_num, and prepare
    /// the slot with our (updated) paxos_seq_num
    ///
    /// Returns the slot index
    async fn prepare_local_slot(&self) -> (usize, PaxosSeqN) {
        // TODO: Fuck, there's a race condition. The vec needs to be locked while I do this. Prob
        // time for an abstraction, the nested locks are getting ugly...
        let mut log = self.log;
        let start = self.next_slot_to_apply.load(Ordering::SeqCst);

        let seq_no = self.next_seq_num(&PaxosSlot::Gap);
        let promised = PaxosSlot::Promised {
            promised_seq_num: seq_no.clone(),
        };
        for i in start..log.len() {
            let mut slot = log[i].write().await;
            if *slot == PaxosSlot::Gap {
                *slot = promised;

                return (i, seq_no);
            }
        }
        log.push(RwLock::new(promised));
        (log.len() - 1, seq_no)
    }

    fn handle_prepare_msg(&self, slot: &mut PaxosSlot, seq_num: PaxosSeqN) -> Option<PaxosRsp> {
        match slot {
            PaxosSlot::Gap => {
                *slot = PaxosSlot::Promised {
                    promised_seq_num: seq_num,
                };
                None
            }
            PaxosSlot::Promised { promised_seq_num } => {
                if seq_num > *promised_seq_num {
                    *promised_seq_num = seq_num;
                    Some(PaxosRsp::Promise {
                        existing_proposal: None,
                    })
                } else {
                    // TODO: send a 'nack as an optimization
                    None
                }
            }
            PaxosSlot::Accepted {
                promised_seq_num,
                accepted_seq_num,
                value,
            } => {
                if seq_num >= *promised_seq_num {
                    *promised_seq_num = seq_num;
                    Some(PaxosRsp::Promise {
                        existing_proposal: Some(Proposal {
                            seq_num: accepted_seq_num.clone(),
                            value: value.clone(),
                        }),
                    })
                } else {
                    // TODO: send a 'nack as an optimization
                    None
                }
            }
            PaxosSlot::Decided { .. } => {
                // TODO: Prob send back the decided value...?
                None
            }
        }
    }

    async fn pick_slot_for_new_transaction(&self) -> (usize, RwLockWriteGuard<PaxosSlot>) {
        todo!()
    }

    async fn phase2(&self, slot_idx: usize, slot: &mut RwLockWriteGuard<'_, PaxosSlot>, proposal: Proposal) -> Phase2Result {
        todo!()
    }

    async fn phase3(&self) {
        tokio::time::sleep(Duration::from_secs(3)).await;
        todo!()
    }
}

impl PaxosSlot {
    /// Try to do a prepare
    ///
    /// If the sequence number is too small, returns an Err with the max sequence number already seen.
    /// Otherwise, return the highest accepted proposal, or None if we haven't accepted any proposals yet
    fn try_prepare(&mut self, prep_n: PaxosSeqN) -> PaxosRsp {
        match self {
            PaxosSlot::Gap => {
                *self = PaxosSlot::Promised {
                    promised_seq_num: prep_n,
                };
                // Ok(None)
                PaxosRsp::Promise { existing_proposal: None }
            }
            PaxosSlot::Promised { promised_seq_num } => {
                if prep_n >= *promised_seq_num { // TODO: Double check >= is OK instaed of >
                    *promised_seq_num = prep_n;
                    PaxosRsp::Promise { existing_proposal: None }
                } else {
                    PaxosRsp::Nack(PaxosNack::NTooLow(promised_seq_num.clone()))
                }
            }
            PaxosSlot::Accepted {
                promised_seq_num,
                accepted_seq_num,
                value,
            } => {
                if prep_n >= *promised_seq_num {
                    *promised_seq_num = prep_n;
                    PaxosRsp::Promise {
                        existing_proposal: Some(Proposal {
                            seq_num: accepted_seq_num.clone(),
                            value: value.clone(),
                        })
                    }
                } else {
                    PaxosRsp::Nack(PaxosNack::NTooLow(promised_seq_num.clone()))
                }
            }
            PaxosSlot::Decided { operation: value } => PaxosRsp::Nack(PaxosNack::AlreadyDecided(value.clone())),
        }
    }

    fn try_propose(&mut self, prop_n: PaxosSeqN, value: LinKvOp) -> PaxosRsp {
        match self {
            PaxosSlot::Gap => {
                *self = PaxosSlot::Accepted {
                    promised_seq_num: prop_n.clone(),
                    accepted_seq_num: prop_n.clone(),
                    value,
                };
                PaxosRsp::AcceptOk
            }
            PaxosSlot::Promised { promised_seq_num } => {
                if prop_n >= *promised_seq_num {
                    *self = PaxosSlot::Accepted {
                        promised_seq_num: prop_n.clone(),
                        accepted_seq_num: prop_n.clone(),
                        value,
                    };
                    PaxosRsp::AcceptOk
                } else {
                    PaxosRsp::Nack(PaxosNack::NTooLow(promised_seq_num.clone()))
                }
            }
            PaxosSlot::Accepted {
                promised_seq_num,
                accepted_seq_num,
                value: cur_val,
            } => {
                if *promised_seq_num < *accepted_seq_num {
                    const S: &str = "promised_seq_num should always be >= accepted_seq_num";
                    error!(%promised_seq_num, %accepted_seq_num, ?value, S);
                    panic!("{}", S);
                }
                if prop_n >= *promised_seq_num {
                    *promised_seq_num = prop_n.clone();
                    *accepted_seq_num = prop_n.clone();
                    *cur_val = value;
                    PaxosRsp::AcceptOk
                } else {
                    PaxosRsp::Nack(PaxosNack::NTooLow(promised_seq_num.clone()))
                }
            }
            PaxosSlot::Decided {
                operation: decided_value,
            } => PaxosRsp::Nack(PaxosNack::AlreadyDecided(decided_value.clone())),
        }
    }

    fn decide(&mut self, value: LinKvOp) {
        if let PaxosSlot::Decided { operation: cur_val } = self {
            if cur_val != &value {
                panic!(
                    "Slot was already decided as {:?} but just tried to change decision to {:?}",
                    cur_val, value
                );
            }
        }

        *self = PaxosSlot::Decided { operation: value }
    }
}
