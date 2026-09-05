use crate::messages::{ErrorCode, LinKvOp, ResponseBody};
use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Debug, Default)]
pub struct LinKvState {
    map: Mutex<HashMap<usize, usize>>,
}

// If this was a serious code base, this layer of the app wouldn't know about the wire request/response DTOs.
// But I'm not being too precious about it today...
impl LinKvState {
    /// Apply an operation to the state machine and return an appropriate response body
    ///
    /// (architecturally returning the response body at this layer is questionable, but this isn't
    /// that serious)
    pub fn apply(&mut self, request_body: LinKvOp) -> ResponseBody {
        match request_body {
            LinKvOp::Read { key } => self.read(key),
            LinKvOp::Write { key, value } => self.write(key, value),
            LinKvOp::Cas { key, from, to } => self.cas(key, from, to),
        }
    }

    fn read(&self, key: usize) -> ResponseBody {
        let map = self.map.lock().unwrap();
        map.get(&key)
            .map(|v| ResponseBody::ReadOk { value: *v })
            .unwrap_or(ResponseBody::Error {
                code: ErrorCode::KeyDoesNotExist,
                text: None,
            })
    }

    fn write(&mut self, key: usize, value: usize) -> ResponseBody {
        self.map.lock().unwrap().insert(key, value);

        ResponseBody::WriteOk
    }

    fn cas(&mut self, key: usize, from: usize, to: usize) -> ResponseBody {
        let mut map = self.map.lock().unwrap();
        let Some(old_val) = map.get_mut(&key) else {
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
