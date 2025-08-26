#!/bin/bash

set -euo pipefail

cargo build --release

trap 'kill -9 $(jobs -p)' EXIT SIGINT SIGTERM

export RUST_BACKTRACE=1

./target/release/queueber --wipe &

sleep 1

./target/release/client stress -p 2 -a 2 -r0 &

wait
