#!/bin/bash -e

cargo b
export WORKLOAD=raft
./maelstrom/maelstrom test -w lin-kv --bin target/debug/maelstrom-rust --nodes n1 --time-limit 10 --log-stderr