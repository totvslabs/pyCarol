#!/bin/sh
set -e

export OSNAME
OSNAME="$(uname | tr '[:upper:]' '[:lower:]')"
export PYTHON
PYTHON=$(command -v python3)
export PIP
PIP=$(command -v pip3)

if ! command -v pip3 >/dev/null 2>&1; then
	test "${OSNAME}" = "linux" && {
		sudo apt-get update
		sudo apt-get install -y python3-distutils
	}
	curl -fL "https://bootstrap.pypa.io/get-pip.py" \
		-o /tmp/get-pip.py
	python3 /tmp/get-pip.py
fi

"${PIP}" install -r requirements.txt
# TODO(amalucelli): bump the versions of each dependency
"${PIP}" install nose coverage nose-cover3 twine

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

test -f ~/.pypirc || {
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
}
