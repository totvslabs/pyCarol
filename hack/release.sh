#!/bin/sh
set -e

test -z "$DEBUG" || set -x

# ensure to checkout to the right tag before creating the image
test -n "${BUILDKITE_TAG}" && {
	git fetch origin
	git fetch origin --tags
	git checkout "tags/${BUILDKITE_TAG}"

	# # push the package to pypi
	make setup package deploy
}