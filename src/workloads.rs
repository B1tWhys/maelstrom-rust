pub mod echo;
pub mod raft;

use serde::Deserialize;

#[derive(Debug, Eq, PartialEq, Deserialize)]
pub enum Workload {
    Echo,
    Raft,
}
