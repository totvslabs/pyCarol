#!/bin/sh
set -e

export OSNAME
OSNAME="$(uname | tr '[:upper:]' '[:lower:]')"

test "${OSNAME}" = "linux" && {
	dpkg -l | grep -q python3-dev || {
		echo "~~~ Installing missing package python3-dev"
		sudo apt-get update
		sudo apt-get install -y python3-dev
	}
}

if ! command -v pip3 >/dev/null 2>&1; then
	test "${OSNAME}" = "linux" && {
		echo "~~~ Installing missing package python3-distutils"
		sudo apt-get update
		sudo apt-get install -y python3-distutils
	}
	echo "~~~ Installing missing package pip3"
	curl -fL "https://bootstrap.pypa.io/get-pip.py" \
		-o /tmp/get-pip.py
	python3 /tmp/get-pip.py
fi

echo "~~~ Installing dependencies from pyCarol"
pip3 --quiet install -r requirements.txt
# TODO(amalucelli): bump the versions of each dependency
pip3 --quiet install nose coverage nose-cover3 twine

mkdir -p bin

if ! command -v sonar-scanner >/dev/null 2>&1; then
	echo "~~~ Installing missing package sonar-scanner"
	test "${OSNAME}" = "darwin" && {
		OSNAME="macosx"
	}
	curl -fL "https://binaries.sonarsource.com/Distribution/sonar-scanner-cli/sonar-scanner-cli-4.2.0.1873-${OSNAME}.zip" \
		-o /tmp/sonar-scanner.zip
	unzip -q /tmp/sonar-scanner.zip -d /tmp
	mv /tmp/sonar-scanner-* ./bin/sonar-scanner
fi

test -f ~/.pypirc && exit 0

echo "~~~ Configuring PyPI"
test -z "${PYPI_USERNAME}" && {
	echo "Please inform the PyPI username:" >&2
	read -r PYPI_USERNAME
}
test -z "${PYPI_USERNAME}" && {
	echo "ERROR: PYPI_USERNAME variable is missing." >&2
	exit 1
}
test -z "${PYPI_PASSWORD}" && {
	echo "Please inform the PyPI password:" >&2
	read -r PYPI_PASSWORD
}
test -z "${PYPI_PASSWORD}" && {
	echo "ERROR: PYPI_PASSWORD variable is missing." >&2
	exit 1
}
cat <<-EOF > ~/.pypirc
	[pypi]
	username = ${PYPI_USERNAME}
	password = ${PYPI_PASSWORD}
EOF
