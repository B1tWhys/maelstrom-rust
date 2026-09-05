#!/bin/bash -e

cargo b
export WORKLOAD=echo
./maelstrom/maelstrom test -w echo --bin target/debug/maelstrom-rust \
  --concurrency 2n \
  --nodes n1 \
  --time-limit 10 \
  --log-stderr