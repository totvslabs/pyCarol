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

	# this is required as we need the desired version on pypi for
	# docker image that will fetch the image from there
	echo "~~~ Waiting for ${BUILDKITE_TAG} to be available on PyPI"
	export COUNTER
	COUNTER=0
	while true; do
		version=$(curl -s -N 'https://pypi.org/project/pycarol/' | grep -oE 'pycarol\s*(\d.+)')
		if [ "pycarol ${BUILDKITE_TAG}" = "${version}" ];  then
			echo Find Version $version
    		break
		fi
		# 5 minutes limit
		test "${COUNTER}" -gt 100 && {
			echo "ERROR: ${BUILDKITE_TAG} couldn't be found in time on PyPI"
			exit 1
		}
		COUNTER=$((COUNTER+1))
		echo $COUNTER
		sleep 3
	done

	# generate and push image to docker hub
	make -e TAG="${BUILDKITE_TAG}" docker hub
}
