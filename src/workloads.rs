pub mod echo;

use serde::Deserialize;

#[derive(Debug, Eq, PartialEq, Deserialize)]
pub enum Workload {
    Echo,
    Raft,
}
