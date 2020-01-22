#!/bin/sh
set -e

export OSNAME
OSNAME="$(uname | tr '[:upper:]' '[:lower:]')"

pip install -r requirements.txt
# TODO(amalucelli): bump the versions of each dependency
pip install nose coverage nose-cover3 twine

mkdir -p bin

if ! command -v sonar-scanner >/dev/null 2>&1; then
	test "${OSNAME}" = "darwin" && {
		OSNAME="macosx"
	}
	curl -fL "https://binaries.sonarsource.com/Distribution/sonar-scanner-cli/sonar-scanner-cli-4.2.0.1873-${OSNAME}.zip" \
		-o /tmp/sonar-scanner.zip
	unzip /tmp/sonar-scanner.zip -d /tmp
	mv /tmp/sonar-scanner-* ./bin/sonar-scanner
fi
