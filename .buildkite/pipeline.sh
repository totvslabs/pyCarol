#!/bin/sh
set -e

cat <<EOF
steps:
    - label: ":buildkite: Build Image"
      command: make docker_ci
      agents:
        name: "${BUILDKITE_AGENT_NAME}"

    - wait: ~

    - label: ":hammer: Tests"
      command: docker run --rm -it -e SONAR_PYCAROL_TOKEN=${SONAR_PYCAROL_TOKEN} -e BUILDKITE_BRANCH=${BUILDKITE_BRANCH} pycarolci make code_scan
      agents:
        name: "${BUILDKITE_AGENT_NAME}"

    - wait: ~

    - label: ":shipit: Deploy"
      command: docker run --rm -it pycarolci make deploy
      branches: "*.*.*"
      agents:
        name: "${BUILDKITE_AGENT_NAME}"

    - wait: ~

    - label: ":docker: pyCarol Image"
      command: make docker
      branches: "*.*.*"
      agents:
        name: "${BUILDKITE_AGENT_NAME}"

    - wait: ~

    - label: ":recycle: Clean up"
      command: docker rmi pycarolci
      agents:
        name: "${BUILDKITE_AGENT_NAME}"
EOF
