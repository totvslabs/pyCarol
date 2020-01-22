#!/bin/sh
set -e

cat <<EOF
steps:
  - label: ":python: Configuring, Packaging, Testing and Scanning"
    command: make ci
    agents:
      queue: "default"
    timeout_in_minutes: 10
  - wait: ~
  - label: ":python: Releasing the Package and Image"
    command: make release
    branches: "*.*.*"
    agents:
      queue: "default"
    timeout_in_minutes: 10
EOF
