#!/bin/sh
set -e

cat <<EOF
steps:
  - label: ":python: Build"
    command: make ci
    agents:
      queue: "default"
    timeout_in_minutes: 10
  - wait: ~
  - label: ":python: Release"
    command: make release
    branches: "*.*.*"
    agents:
      queue: "default"
    timeout_in_minutes: 10
EOF
