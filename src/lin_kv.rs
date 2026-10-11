use crate::messages::{ErrorCode, LinKvOp, ResponseBody};
use std::collections::HashMap;

#[derive(Debug, Default)]
pub struct LinKvState {
    map: HashMap<usize, usize>,
    /// Number of transactions applied so far. This is the same as saying the next slot id to use
    tx_count: usize,
}

// If this was a serious code base, this layer of the app wouldn't know about the wire request/response DTOs.
// But I'm not being too precious about it today...
impl LinKvState {
    pub fn get_tx_count(&self) -> usize {
        self.tx_count
    }

    /// Apply an operation to the state machine and return an appropriate response body
    ///
    /// (architecturally returning the response body at this layer is questionable, but this isn't
    /// that serious)
    pub fn apply(&mut self, request_body: LinKvOp) -> ResponseBody {
        self.tx_count += 1;
        match request_body {
            LinKvOp::Read { key, .. } => self.read(key),
            LinKvOp::Write { key, value, .. } => self.write(key, value),
            LinKvOp::Cas { key, from, to, .. } => self.cas(key, from, to),
        }
    }

    fn read(&self, key: usize) -> ResponseBody {
        self.map
            .get(&key)
            .map(|v| ResponseBody::ReadOk { value: *v })
            .unwrap_or(ResponseBody::Error {
                code: ErrorCode::KeyDoesNotExist,
                text: None,
            })
    }

    fn write(&mut self, key: usize, value: usize) -> ResponseBody {
        self.map.insert(key, value);

        ResponseBody::WriteOk
    }

    fn cas(&mut self, key: usize, from: usize, to: usize) -> ResponseBody {
        let Some(old_val) = self.map.get_mut(&key) else {
            return ResponseBody::Error {
                code: ErrorCode::KeyDoesNotExist,
                text: Some(format!("Key not found: {}", key)),
            };
        };

        if old_val != &from {
            return ResponseBody::Error {
                code: ErrorCode::PreconditionFailed,
                text: Some(format!(
                    "Current value didn't match the expected value. From: {:?}, actual current: {:?}",
                    from, old_val,
                )),
            };
        }

        *old_val = to;

        ResponseBody::CasOk
    }
}
