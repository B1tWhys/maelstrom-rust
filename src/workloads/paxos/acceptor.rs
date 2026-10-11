use crate::messages::{LinKvOp, PaxosNack, PaxosReq, PaxosRsp, Proposal, UniquePaxosId};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::error;

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum AcceptorSlotState {
    New {
        slot_idx: usize,
        // last_promised_seq_num: Option<PaxosSeqN>,
    },
    Promised {
        slot_idx: usize,
        promised_seq_num: UniquePaxosId,
    },
    Accepted {
        slot_idx: usize,
        promised_seq_num: UniquePaxosId,
        accepted_seq_num: UniquePaxosId,
        value: LinKvOp,
    },
}

impl AcceptorSlotState {
    pub fn slot_idx(&self) -> usize {
        match self {
            AcceptorSlotState::New { slot_idx, .. }
            | AcceptorSlotState::Promised { slot_idx, .. }
            | AcceptorSlotState::Accepted { slot_idx, .. } => *slot_idx,
        }
    }

    pub fn try_prepare(&mut self, prep_n: UniquePaxosId) -> PaxosRsp {
        match self {
            AcceptorSlotState::New { slot_idx: idx, .. } => {
                *self = AcceptorSlotState::Promised {
                    slot_idx: *idx,
                    promised_seq_num: prep_n,
                };
                // Ok(None)
                PaxosRsp::Promise {
                    existing_proposal: None,
                }
            }
            AcceptorSlotState::Promised {
                slot_idx: _,
                promised_seq_num,
            } => {
                if prep_n > *promised_seq_num {
                    *promised_seq_num = prep_n;
                    PaxosRsp::Promise {
                        existing_proposal: None,
                    }
                } else {
                    PaxosRsp::Nack(PaxosNack::NTooLow(promised_seq_num.clone()))
                }
            }
            AcceptorSlotState::Accepted {
                slot_idx,
                promised_seq_num,
                accepted_seq_num,
                value,
            } => {
                if prep_n >= *promised_seq_num {
                    *promised_seq_num = prep_n;
                    PaxosRsp::Promise {
                        existing_proposal: Some(Proposal {
                            slot_idx: *slot_idx,
                            seq_num: accepted_seq_num.clone(),
                            value: value.clone(),
                        }),
                    }
                } else {
                    PaxosRsp::Nack(PaxosNack::NTooLow(promised_seq_num.clone()))
                }
            }
        }
    }

    pub fn try_propose(&mut self, Proposal { seq_num, value, .. }: Proposal) -> PaxosRsp {
        match self {
            AcceptorSlotState::New { slot_idx } => {
                *self = AcceptorSlotState::Accepted {
                    slot_idx: *slot_idx,
                    promised_seq_num: seq_num.clone(),
                    accepted_seq_num: seq_num.clone(),
                    value,
                };
                PaxosRsp::AcceptOk
            }
            AcceptorSlotState::Promised {
                slot_idx,
                promised_seq_num,
            } => {
                if seq_num >= *promised_seq_num {
                    *self = AcceptorSlotState::Accepted {
                        slot_idx: *slot_idx,
                        promised_seq_num: seq_num.clone(),
                        accepted_seq_num: seq_num.clone(),
                        value,
                    };
                    PaxosRsp::AcceptOk
                } else {
                    PaxosRsp::Nack(PaxosNack::NTooLow(promised_seq_num.clone()))
                }
            }
            AcceptorSlotState::Accepted {
                slot_idx: _,
                promised_seq_num,
                accepted_seq_num,
                value: cur_val,
            } => {
                if promised_seq_num < accepted_seq_num {
                    const ERR_MSG: &str = "promised_seq_num should always be >= accepted_seq_num";
                    error!(%promised_seq_num, %accepted_seq_num, ?value, ERR_MSG);
                    panic!("{}", ERR_MSG);
                }
                if seq_num >= *promised_seq_num {
                    *promised_seq_num = seq_num.clone();
                    *accepted_seq_num = seq_num.clone();
                    *cur_val = value;
                    PaxosRsp::AcceptOk
                } else {
                    PaxosRsp::Nack(PaxosNack::NTooLow(promised_seq_num.clone()))
                }
            }
        }
    }
}

#[derive(Debug, Default)]
pub struct Acceptor {
    log: Vec<Arc<RwLock<AcceptorSlotState>>>,
}

impl Acceptor {
    pub async fn handle_paxos_req(&mut self, paxos_req: PaxosReq) -> PaxosRsp {
        match paxos_req {
            PaxosReq::Prepare { seq_num, slot_idx } => {
                let slot = self.get_or_create_slot(slot_idx).await;
                slot.write().await.try_prepare(seq_num)
            }
            PaxosReq::Propose {
                seq_num,
                slot_idx,
                value,
            } => {
                let slot = self.get_or_create_slot(slot_idx).await;
                slot.write().await.try_propose(Proposal {
                    seq_num,
                    value,
                    slot_idx,
                })
            }
        }
    }

    async fn get_or_create_slot(&mut self, i: usize) -> &Arc<RwLock<AcceptorSlotState>> {
        while self.log.len() <= i {
            let slot = AcceptorSlotState::New {
                slot_idx: i,
                // last_promised_seq_num: None,
            };
            self.log.push(Arc::new(RwLock::new(slot)));
        }

        &self.log[i]
    }
}
