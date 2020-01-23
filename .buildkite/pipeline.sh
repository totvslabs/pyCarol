#!/bin/sh
set -e

cat <<EOF
steps:
  - label: ":python: build"
    command: make ci
    agents:
      queue: "default"
    timeout_in_minutes: 10
  - wait: ~
  - label: ":python: release"
    command: make release
    branches: "*.*.*"
    agents:
      queue: "default"
    timeout_in_minutes: 15
EOF
