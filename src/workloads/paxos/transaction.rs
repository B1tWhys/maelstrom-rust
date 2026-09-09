use crate::messages::{
    LinKvOp, PaxosNack, PaxosReq, PaxosRsp, PaxosSeqN, Proposal, RequestBody, ResponseBody,
};
use crate::node_ctx::NodeCtx;
use crate::workloads::paxos::log::{ClaimedSlot, Log};
use crate::workloads::paxos::paxos_slot::PaxosSlot;
use std::ops::Deref;
use std::sync::Arc;
use tokio::task::JoinSet;
use tracing::{debug, info};

pub struct Transaction {
    node_ctx: Arc<NodeCtx>,
    op: LinKvOp,
    slot: Option<ClaimedSlot>,
    paxos_phase: PaxosPhase,
    log: Arc<Log>,
}

enum PaxosPhase {
    /// Prepare
    Phase1,
    /// Propose
    Phase2(Proposal),
    /// Decided
    Phase3,
}

impl Transaction {
    pub fn new(_log: &Log, _op: LinKvOp) -> Self {
        todo!()
    }

    pub async fn run(mut self) -> ResponseBody {
        loop {
            match &self.paxos_phase {
                PaxosPhase::Phase1 => self.phase_1().await,
                PaxosPhase::Phase2(p) => self.phase_2(p.clone()).await,
                PaxosPhase::Phase3 => {
                    return self.phase_3().await;
                }
            }
        }
    }

    // There's a spot in this function that `tokio::spawn`s a new transaction and calls `run.await`.
    // This is a recursive indirect async function call (it's indirect b/c it goes through the
    // `run()` function). There's a limitation in the current rust trait solver which prevents
    // it from realizing that the generated state machine for this phase_1 function is Send (it
    // can't trace blanket impl's through multiple layers of opaque types. Or something like that).
    //
    // This shouldn't be necessary on the next generation trait solver (which I think landed
    // on nightly recently), but for now we'll stick with manually desugaring the async fn definition
    // and specifying the Send trait bound ourselves, to help the trait solver figure things out.
    // This is still sound an whatnot, it's just rather ugly.
    //
    // This was miserable to figure out. For more info, see:
    // https://github.com/rust-lang/rust/issues/135062
    #[allow(clippy::manual_async_fn)]
    fn phase_1(&mut self) -> impl Future<Output = ()> + Send + '_ {
        async move {
            if self.slot.is_none() {
                self.slot = Some(self.log.claim_slot().await);
            }

            let (slot_idx, last_seq_no) = {
                let slot = self.slot.as_ref().unwrap().read().await;
                let sn = match slot.deref() {
                    PaxosSlot::New {
                        last_promised_seq_num,
                        ..
                    } => last_promised_seq_num.clone(),
                    PaxosSlot::Promised {
                        promised_seq_num, ..
                    }
                    | PaxosSlot::Accepted {
                        promised_seq_num, ..
                    } => Some(promised_seq_num.clone()),
                    PaxosSlot::Decided { .. } => {
                        // By some miracle, the slot was decided between us claiming it and us trying to
                        // come up with a sequence number. We can't use this slot for the transaction,
                        // just start over from the top instead with a new seq number.
                        drop(slot);
                        self.slot = None;
                        return;
                    }
                };
                (slot.slot_idx(), sn)
            };

            let next_seq_no = PaxosSeqN {
                counter: last_seq_no.map(|n| n.counter + 1).unwrap_or_default(),
                node_id: self.node_ctx.node_id.clone(),
            };

            debug!(slot=?self.slot, %next_seq_no, "Prepping slot");

            // Send async prep requests to all acceptors
            let mut prep_req_tasks = JoinSet::new();
            for dest in self.node_ctx.other_nodes().cloned() {
                let req = RequestBody::Paxos(PaxosReq::Prepare {
                    seq_num: next_seq_no.clone(),
                    slot_idx,
                });
                let node_ctx = Arc::clone(&self.node_ctx);

                // TODO: timeout
                prep_req_tasks.spawn(async move { node_ctx.send_request(dest, req).await });
            }

            let mut responses = vec![];

            {
                // prep the local slot. Don't hold the write lock longer than necessary
                let mut slot = self.slot.as_ref().unwrap().write().await;
                let resp = slot.try_prepare(next_seq_no.clone());
                responses.push(resp);
            }

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
                        debug!(%e, "Failed to prepare an acceptor. No biggie");
                    }
                }
                // We only need responses from a simple majority of nodes (including ourself). We can stop
                // early once we get to that point
                if responses.len() == self.node_ctx.min_quorum_size() {
                    break;
                }
            }
            // Cancel any requests that are in-flight still
            drop(prep_req_tasks);

            // If we didn't get the necessary number of responses, give up on this round of prep'ing.
            // We can try again with the same slot, and a new sequence number
            if responses.len() < self.node_ctx.min_quorum_size() {
                info!(
                    prep_rsp_count = responses.len(),
                    "Phase 1 timed out attempting to gather promises"
                );
                return;
            }

            let decided_val = responses
                .iter()
                .filter_map(|r| {
                    if let PaxosRsp::Nack(PaxosNack::AlreadyDecided(v)) = r {
                        Some(v)
                    } else {
                        None
                    }
                })
                .next();
            let higher_existing_prop_num = responses
                .iter()
                .filter_map(|r| {
                    if let PaxosRsp::Nack(PaxosNack::NTooLow(n)) = r {
                        Some(n)
                    } else {
                        None
                    }
                })
                .max();
            let max_existing_proposal_val = responses
                .iter()
                .filter_map(|r| {
                    if let PaxosRsp::Promise {
                        existing_proposal: Some(p),
                    } = r
                    {
                        Some(p.clone())
                    } else {
                        None
                    }
                })
                .max_by_key(|p| p.seq_num.clone())
                .map(|p| p.value);

            if let Some(decided) = decided_val {
                // If this slot has already been decided (and we just haven't been notified about it),
                // then update our local slot state and restart the transaction with a new slot
                self.slot
                    .take()
                    .unwrap()
                    .write()
                    .await
                    .decide(decided.clone());
            } else if let Some(max_seq_num) = higher_existing_prop_num {
                // Someone already did a prepare (or accept) with a higher seq number than us. Update
                // our slot's sequence number and try again

                match &mut *self.slot.as_ref().unwrap().write().await {
                    PaxosSlot::New {
                        last_promised_seq_num,
                        ..
                    } => {
                        *last_promised_seq_num =
                            last_promised_seq_num.take().max(Some(max_seq_num.clone()))
                    }
                    PaxosSlot::Promised {
                        promised_seq_num, ..
                    }
                    | PaxosSlot::Accepted {
                        promised_seq_num, ..
                    } => {
                        *promised_seq_num = max_seq_num.clone();
                    }
                    PaxosSlot::Decided { .. } => {
                        // Somehow while we were doing our prepares, this slot was decided and we were notified.
                        // Don't need to do anything here, we'll try the prepare again and land in the "already decided"
                        // case above, and then we'll try again with a new slot.
                    }
                }
            } else if let Some(old_prop_val) = max_existing_proposal_val
                && old_prop_val != self.op
            {
                // Apparently there was already a transaction that got at least part way (maybe all the way) done
                // with this slot before stopping (or maybe it's still going). We need to propose the value
                // from that transaction in this slot, and use a new slot for our intended value.
                //
                // So... spin up a new Transaction to pick up from this point with this existing value,
                // and clear our slot so that we pick a new slot for the next attempt

                let new_tx = Transaction {
                    node_ctx: Arc::clone(&self.node_ctx),
                    op: old_prop_val.clone(),
                    slot: self.slot.take(),
                    paxos_phase: PaxosPhase::Phase2(Proposal {
                        seq_num: next_seq_no,
                        value: old_prop_val.clone(),
                    }),
                    log: Arc::clone(&self.log),
                };
                tokio::spawn(async move {
                    let tx = new_tx;
                    tx.run().await;
                });
            } else {
                // We're clear to propose the value we want! Hooray!
                responses.iter().for_each(|r| {
                    // sanity check...
                    assert_eq!(
                        r,
                        &PaxosRsp::Promise {
                            existing_proposal: None
                        }
                    )
                });

                self.paxos_phase = PaxosPhase::Phase2(Proposal {
                    seq_num: next_seq_no,
                    value: self.op.clone(),
                });
            }
        }
    }

    async fn phase_2(&mut self, proposal: Proposal) {
        // FIXME: this is mostly dupe code with phase_1. Could be cleaned up a lot
        let mut req_tasks = JoinSet::new();
        for dest in self.node_ctx.other_nodes().cloned() {
            let req = RequestBody::Paxos(PaxosReq::Propose {
                seq_num: proposal.seq_num.clone(),
                slot_idx: self.slot.as_ref().unwrap().read().await.slot_idx(),
                value: proposal.value.clone(),
            });
            let node_ctx = Arc::clone(&self.node_ctx);
            req_tasks.spawn(async move { node_ctx.send_request(dest, req).await });
        }

        let mut responses = vec![];

        {
            let mut slot = self.slot.as_ref().unwrap().write().await;
            responses.push(slot.try_propose(proposal.clone()));
        }
        while let Some(resp) = req_tasks.join_next().await {
            // actually I figured it out. It's if the task panicked. We should prob panic too.
            let resp = resp.expect("Not sure why this can fail...");
            match resp {
                Ok(ResponseBody::Paxos(rsp)) => {
                    responses.push(rsp);
                }
                Ok(r) => {
                    panic!("Non-paxos response received for accept request: {r:?}");
                }
                Err(e) => {
                    debug!(%e, "Failed to propose to an acceptor. No biggie");
                }
            }

            if responses.len() == self.node_ctx.min_quorum_size() {
                break;
            }
        }

        drop(req_tasks);

        // If we didn't get the necessary number of responses, give up on this round of prep'ing.
        // We can try again with the same slot, and a new sequence number
        if responses.len() < self.node_ctx.min_quorum_size() {
            info!(
                prep_rsp_count = responses.len(),
                "Phase 2 timed out attempting to make proposals"
            );
            return;
        }

        let decided_val = responses
            .iter()
            .filter_map(|r| {
                if let PaxosRsp::Nack(PaxosNack::AlreadyDecided(v)) = r {
                    Some(v)
                } else {
                    None
                }
            })
            .next();
        let max_higher_n = responses
            .iter()
            .filter_map(|r| match r {
                PaxosRsp::Nack(PaxosNack::NTooLow(n)) => Some(n),
                _ => None,
            })
            .max();

        if let Some(decided) = decided_val
            && decided != &self.op
        {
            // If this slot has already been decided (and we just haven't been notified about it),
            // then update our local slot state and restart the transaction with a new slot
            self.slot
                .take()
                .unwrap()
                .write()
                .await
                .decide(decided.clone());
        } else if let Some(p) = max_higher_n {
            // Update our local sequence number, and then try again from phase 1
            match &mut *self.slot.as_ref().unwrap().write().await {
                PaxosSlot::New {
                    last_promised_seq_num,
                    ..
                } => {
                    *last_promised_seq_num = last_promised_seq_num.take().max(Some(p.clone()));
                }
                PaxosSlot::Promised {
                    promised_seq_num, ..
                }
                | PaxosSlot::Accepted {
                    promised_seq_num, ..
                } => {
                    *promised_seq_num = (&*promised_seq_num).max(p).clone();
                }
                PaxosSlot::Decided { .. } => {}
            }
            self.paxos_phase = PaxosPhase::Phase1;
        } else {
            assert!(
                responses.iter().all(|r| matches!(r, PaxosRsp::AcceptOk)),
                "All responses should have been either one of the above states, or AcceptOk. Responses were: {responses:?}"
            );
            self.paxos_phase = PaxosPhase::Phase3;
        }
    }

    async fn phase_3(&mut self) -> ResponseBody {
        todo!()
    }
}
