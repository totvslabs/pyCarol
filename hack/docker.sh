#!/bin/sh
set -e

# ensure to checkout to the right tag before creating the image
test -n "${BUILDKITE_TAG}" && {
	git fetch origin
	git fetch origin --tags
	git checkout "tags/${BUILDKITE_TAG}"
	make -e TAG="${BUILDKITE_TAG}" docker_build docker_push
}
