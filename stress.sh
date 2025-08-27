#!/bin/bash

cargo build --release

trap 'kill -9 $(jobs -p)' EXIT SIGINT SIGTERM

export RUST_BACKTRACE=1

SERVER_ARGS=${SERVER_ARGS:-}
./target/release/queueber --wipe $SERVER_ARGS &
server_pid=$!

sleep 1

if [ $# -eq 0 ]; then
    args=(
        -p 2
        -a 2
        -r0
        -d 10
    )
else
    args=("$@")
fi

echo "Running client with args: ${args[*]}"

./target/release/client stress "${args[@]}" &
client_pid=$!

wait $client_pid
client_exit_code=$?

echo "Waiting for server to exit..."
(
    sleep 10
    kill -9 $server_pid
) &
wait $server_pid
server_exit_code=$?


echo "Server exited with code $server_exit_code"
echo "Client exited with code $client_exit_code"

if [ $server_exit_code -ne 0 -o $client_exit_code -ne 0 ]; then
    exit $server_exit_code
fi
