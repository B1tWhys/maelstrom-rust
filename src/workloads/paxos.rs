#![allow(dead_code)] // FIXME
mod acceptor;
pub mod proposer;

use crate::lin_kv::LinKvState;
use crate::messages::{
    ErrorCode, LinKvOp, Message, MessageBody, PaxosReq, RequestBody, ResponseBody, UniquePaxosId,
};
use crate::node_ctx::NodeCtx;
use crate::workloads::paxos::acceptor::Acceptor;
use crate::workloads::paxos::proposer::propose;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::{Mutex, RwLock};
use tracing::info;

/// Represents a node in a cluster of hosts running paxos. Encapsulates all that logic & state.
pub struct PaxosNode {
    node_ctx: Arc<NodeCtx>,
    /// The titular state machine (in this case a kv store, later maybe I'll make this generic). Decided
    /// [LinKvOp]s are applied to the state.
    state_machine: Arc<RwLock<LinKvState>>,

    acceptor: Arc<Mutex<Acceptor>>,

    op_id_counter: Arc<AtomicUsize>,

    /// Only one client request is processed at a time. Only the holder of this lock is allowed to
    /// call propose... in fact maybe this should be in the proposer module, come to think of it...
    proposing_lock: Arc<Mutex<()>>,
}

// ===== Message handling/overall plumbing =====
impl PaxosNode {
    pub fn new(node_ctx: Arc<NodeCtx>) -> Self {
        let cur_leader = node_ctx
            .nodes
            .iter()
            .min()
            .cloned()
            .expect("NodeCtx has no nodes! It's gonna be hard to do paxos without them!");

        info!("Assuming {cur_leader} is leader node (TODO: Do this through an election)");

        Self {
            node_ctx,
            state_machine: Default::default(),
            acceptor: Default::default(),
            op_id_counter: Default::default(),
            proposing_lock: Default::default(),
        }
    }

    pub async fn run(self: Arc<Self>) -> anyhow::Result<()> {
        while let Some(message) = self.node_ctx.recv_request().await {
            let self_arc = Arc::clone(&self);
            tokio::spawn(async move {
                let resp = self_arc.route_request(&message).await;
                self_arc.node_ctx.send_response(&message, resp).await;
            });
        }

        Ok(())
    }

    async fn route_request(&self, message: &Message) -> ResponseBody {
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

        match req_body {
            RequestBody::LinKvOp(op) => self.handle_client_req(op.clone()).await,
            RequestBody::Paxos(paxos_req) => {
                self.handle_internal_paxos_req(paxos_req.clone()).await
            }
            _ => ResponseBody::Error {
                code: ErrorCode::NotSupported,
                text: Some(format!("paxos doesn't support request: {:?}", message)),
            },
        }
    }

    fn new_op_id(&self) -> UniquePaxosId {
        UniquePaxosId {
            n: self.op_id_counter.fetch_add(1, Ordering::SeqCst),
            node_id: self.node_ctx.node_id.clone(),
        }
    }
}

// ===== Actual Paxos implementation =====

impl PaxosNode {
    async fn handle_client_req(&self, mut op: LinKvOp) -> ResponseBody {
        let _guard = self.proposing_lock.lock().await;

        // Make this operation distinct from another request to another host in the cluster that happens
        // to do the same thing. This is necessary for any non-idempotent operations (i.e. Cas).
        op.set_op_id(self.new_op_id());

        // If there's only 1 node, we can just make the change to our snapshot. Ezpz!
        if self.node_ctx.nodes.len() == 1 {
            let mut snapshot = self.state_machine.write().await;
            return snapshot.apply(op.clone());
        }

        // Explicit scope to make sure the read lock isn't held longer than this line, which could
        // cause deadlocks later
        let mut slot_idx = { self.state_machine.read().await.get_tx_count() };

        loop {
            let chosen_op = propose(&self.node_ctx, op.clone(), slot_idx).await;
            let result = self.state_machine.write().await.apply(chosen_op.clone());
            if chosen_op == op {
                return result;
            } else {
                slot_idx += 1;
            }
        }
    }

    async fn handle_internal_paxos_req(&self, paxos_req: PaxosReq) -> ResponseBody {
        let paxos_resp = self.acceptor.lock().await.handle_paxos_req(paxos_req).await;
        ResponseBody::Paxos(paxos_resp)
    }
}
