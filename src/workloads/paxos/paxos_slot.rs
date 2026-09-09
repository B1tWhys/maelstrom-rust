use crate::messages::{LinKvOp, PaxosNack, PaxosRsp, PaxosSeqN, Proposal, ResponseBody};
use tracing::error;

/// State machine for 1 round of paxos
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum PaxosSlot {
    New {
        slot_idx: usize,
        last_promised_seq_num: Option<PaxosSeqN>,
    },
    Promised {
        slot_idx: usize,
        promised_seq_num: PaxosSeqN,
    },
    Accepted {
        slot_idx: usize,
        promised_seq_num: PaxosSeqN,
        accepted_seq_num: PaxosSeqN,
        value: LinKvOp,
    },
    Decided {
        slot_idx: usize,
        operation: LinKvOp,
        result: ResponseBody,
    },
}

impl PaxosSlot {
    pub fn slot_idx(&self) -> usize {
        match self {
            PaxosSlot::New { slot_idx, .. }
            | PaxosSlot::Promised { slot_idx, .. }
            | PaxosSlot::Accepted { slot_idx, .. }
            | PaxosSlot::Decided { slot_idx, .. } => *slot_idx,
        }
    }

    /// Try to do a prepare
    ///
    /// If the sequence number is too small, returns an Err with the max sequence number already seen.
    /// Otherwise, return the highest accepted proposal, or None if we haven't accepted any proposals yet
    pub fn try_prepare(&mut self, prep_n: PaxosSeqN) -> PaxosRsp {
        match self {
            PaxosSlot::New { slot_idx: idx, .. } => {
                *self = PaxosSlot::Promised {
                    slot_idx: *idx,
                    promised_seq_num: prep_n,
                };
                // Ok(None)
                PaxosRsp::Promise {
                    existing_proposal: None,
                }
            }
            PaxosSlot::Promised {
                slot_idx: _,
                promised_seq_num,
            } => {
                if prep_n >= *promised_seq_num {
                    // TODO: Double check >= is OK instaed of >
                    *promised_seq_num = prep_n;
                    PaxosRsp::Promise {
                        existing_proposal: None,
                    }
                } else {
                    PaxosRsp::Nack(PaxosNack::NTooLow(promised_seq_num.clone()))
                }
            }
            PaxosSlot::Accepted {
                slot_idx: _,
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
                        }),
                    }
                } else {
                    PaxosRsp::Nack(PaxosNack::NTooLow(promised_seq_num.clone()))
                }
            }
            PaxosSlot::Decided {
                operation: value,
                result: _,
                ..
            } => PaxosRsp::Nack(PaxosNack::AlreadyDecided(value.clone())),
        }
    }

    pub fn try_propose(&mut self, Proposal { seq_num, value }: Proposal) -> PaxosRsp {
        match self {
            PaxosSlot::New {
                slot_idx,
                last_promised_seq_num,
            } => {
                if let Some(n) = last_promised_seq_num
                    && &seq_num < n
                {
                    return PaxosRsp::Nack(PaxosNack::NTooLow(n.clone()));
                }

                *self = PaxosSlot::Accepted {
                    slot_idx: *slot_idx,
                    promised_seq_num: seq_num.clone(),
                    accepted_seq_num: seq_num.clone(),
                    value,
                };
                PaxosRsp::AcceptOk
            }
            PaxosSlot::Promised {
                slot_idx,
                promised_seq_num,
            } => {
                if seq_num >= *promised_seq_num {
                    *self = PaxosSlot::Accepted {
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
            PaxosSlot::Accepted {
                slot_idx: _,
                promised_seq_num,
                accepted_seq_num,
                value: cur_val,
            } => {
                if *promised_seq_num < *accepted_seq_num {
                    const S: &str = "promised_seq_num should always be >= accepted_seq_num";
                    error!(%promised_seq_num, %accepted_seq_num, ?value, S);
                    panic!("{}", S);
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
            PaxosSlot::Decided {
                operation: decided_value,
                ..
            } => PaxosRsp::Nack(PaxosNack::AlreadyDecided(decided_value.clone())),
        }
    }

    pub fn decide(&mut self, _value: LinKvOp) {
        todo!();
        // if let PaxosSlot::Decided { operation: cur_val } = self {
        //     if cur_val != &value {
        //         panic!(
        //             "Slot was already decided as {:?} but just tried to change decision to {:?}",
        //             cur_val, value
        //         );
        //     }
        // }
        //
        // *self = PaxosSlot::Decided { operation: value }
    }
}
