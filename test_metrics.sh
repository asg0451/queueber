#!/bin/bash

# Test script for Prometheus metrics

echo "Starting queueber server with metrics..."
# Start the server in the background
cargo run --bin queueber -- --metrics 127.0.0.1:9091 &
SERVER_PID=$!

# Wait for server to start
sleep 3

echo "Testing metrics endpoint..."
# Test the metrics endpoint
curl -s http://127.0.0.1:9091/metrics | head -20

echo -e "\nTesting health endpoint..."
# Test the health endpoint
curl -s http://127.0.0.1:9091/health

echo -e "\n\nMetrics output (first 50 lines):"
curl -s http://127.0.0.1:9091/metrics | head -50

# Clean up
echo -e "\n\nStopping server..."
kill $SERVER_PID
wait $SERVER_PID 2>/dev/null

echo "Test completed!"