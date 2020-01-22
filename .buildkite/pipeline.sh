#!/bin/bash

# Outputs a pipeline that targets agents that have the same 'name' meta-data
# value as the step that does the pipeline upload. This means that all the
# steps will run on the same agent machine, assuming that the 'name' meta-data
# value is unique to each agent.
#
# Each agent needs to be configured with meta-data like so:
#
# meta-data="name=<unique-name>"
#
# To use, save this file as .buildkite/pipeline.sh, chmod +x, and then set your
# first pipeline step to run this and pipe it into pipeline upload:
#
# .buildkite/pipeline.sh | buildkite-agent pipeline upload
#

agent_name=$(buildkite-agent meta-data get name)

cat << EOF
steps:
    - label: ":buildkite: Build Image"
      command: make docker_ci
      agents:
        name: "${agent_name}"

    - wait: ~

    - label: ":hammer: Tests"
      command: docker run --rm -it -e SONAR_PYCAROL_TOKEN=${SONAR_PYCAROL_TOKEN} -e BUILDKITE_BRANCH=${BUILDKITE_BRANCH} pycarolci make code_scan
      agents:
        name: "${agent_name}"

    - wait: ~

    - label: ":shipit: Deploy"
      command: docker run --rm -it pycarolci make deploy
      branches: "*.*.*"
      agents:
        name: "${agent_name}"

    - wait: ~

    - label: ":docker: pyCarol Image"
      command: make docker
      branches: "*.*.*"
      agents:
        name: "${agent_name}"

    - wait: ~

    - label: ":recycle: Clean up"
      command: docker rmi pycarolci
      agents:
        name: "${agent_name}"

EOF
