use crate::messages::{
    LinKvOp, PaxosNack, PaxosReq, PaxosRsp, Proposal, RequestBody, ResponseBody, UniquePaxosId,
};
use crate::node_ctx::NodeCtx;
use std::sync::Arc;
use tokio::task::JoinSet;
use tracing::debug;

enum PaxosPhase {
    Prepare(Proposal),
    Propose(Proposal),
    Decided(LinKvOp),
}

/// Proposes a LinKvOp value for a given slot. Returns the value that was actually accepted for that
/// slot (which may or may not be what we wanted to get chosen).
///
/// This will retry proposing for the given slot until a value is chosen. It's the caller's
/// responsibility to time out, and/or to retry with a new slot if the chosen value isn't what was
/// intended
pub async fn propose(node_ctx: &Arc<NodeCtx>, op: LinKvOp, slot_idx: usize) -> LinKvOp {
    let seq_num = UniquePaxosId::new(&node_ctx, 0);
    let mut state = PaxosPhase::Prepare(Proposal {
        slot_idx,
        seq_num,
        value: op,
    });

    loop {
        state = match state {
            PaxosPhase::Prepare(proposal) => phase1::phase1(node_ctx, proposal).await,
            PaxosPhase::Propose(proposal) => phase2::phase2(node_ctx, proposal).await,
            PaxosPhase::Decided(result) => {
                return result;
            }
        }
    }
}

async fn send_to_quorum(node_ctx: &Arc<NodeCtx>, req: RequestBody) -> Vec<PaxosRsp> {
    let mut tasks = JoinSet::new();

    for node in node_ctx.nodes.clone() {
        let r = req.clone();
        let ctx = Arc::clone(node_ctx);
        tasks.spawn(async move { ctx.send_request(node, r).await });
    }

    let mut responses = vec![];
    let min_quorum_size = node_ctx.min_quorum_size();
    while let Some(resp) = tasks.join_next().await {
        let resp =
            resp.expect("The task panicked or got canceled somehow. This should never happen");

        match resp {
            Ok(ResponseBody::Paxos(r)) => {
                responses.push(r);
            }
            Ok(r) => {
                panic!("Non-paxos response received for paxos request: {r:?}");
            }
            Err(e) => {
                debug!(%e, "Failed to hear back from an acceptor. No biggie");
            }
        }
        if responses.len() >= min_quorum_size {
            break;
        }
    }

    // TODO: handle this more elegantly
    if responses.len() < min_quorum_size {
        panic!(
            "Somehow heard back from a quorum of nodes, but they were all invalid responses somehow?"
        );
    }

    responses
}

mod phase1 {
    use super::*;

    pub async fn phase1(node_ctx: &Arc<NodeCtx>, mut proposal: Proposal) -> PaxosPhase {
        let req = RequestBody::Paxos(PaxosReq::Prepare {
            seq_num: proposal.seq_num.clone(),
            slot_idx: proposal.slot_idx,
        });
        let responses = send_to_quorum(node_ctx, req).await;

        // Handle higher sequence number responses
        let max_higher_n = responses
            .iter()
            .filter_map(|rsp| match rsp {
                PaxosRsp::Nack(PaxosNack::NTooLow(n)) => Some(n),
                PaxosRsp::Promise { .. } | PaxosRsp::AcceptOk => None,
            })
            .max();
        if let Some(max_conflicting_seq_no) = max_higher_n {
            // There was already a promise with a higher sequence number than ours. Try preparing again
            // with an even higher sequence number than that
            proposal.seq_num.n = max_conflicting_seq_no.n + 1;
            // TODO: Sleep w/ jitter
            return PaxosPhase::Prepare(proposal);
        }

        let val_2_propose = responses
            .iter()
            .filter_map(|rsp| match rsp {
                PaxosRsp::Promise { existing_proposal } => existing_proposal.as_ref(),
                _ => None,
            })
            .max_by_key(|p| &p.seq_num)
            .map(|p| &p.value)
            .unwrap_or(&proposal.value)
            .clone();

        proposal.value = val_2_propose;
        PaxosPhase::Propose(proposal)
    }
}

mod phase2 {
    use super::*;

    pub async fn phase2(node_ctx: &Arc<NodeCtx>, mut proposal: Proposal) -> PaxosPhase {
        let req = RequestBody::Paxos(PaxosReq::Propose {
            seq_num: proposal.seq_num.clone(),
            slot_idx: proposal.slot_idx,
            value: proposal.value.clone(),
        });
        let responses = send_to_quorum(node_ctx, req).await;

        let max_higher_n = responses
            .iter()
            .filter_map(|r| match r {
                PaxosRsp::Nack(PaxosNack::NTooLow(n)) => Some(n),
                _ => None,
            })
            .max();

        if let Some(UniquePaxosId { n, .. }) = max_higher_n {
            // We got scooped by another node preparing this slot. Now we need to go back to preparing
            // with another, even higher, sequence number.
            proposal.seq_num.n = n + 1;
            return PaxosPhase::Prepare(proposal);
        }

        assert!(
            responses.iter().all(|r| matches!(r, PaxosRsp::AcceptOk)),
            "All responses should have been either AcceptOk or Nack. Actual responses were: {responses:?}"
        );

        PaxosPhase::Decided(proposal.value)
    }
}
