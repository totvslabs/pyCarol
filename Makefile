.PHONY: help clean dev docs package test deploy setup_pypi ci docker_ci docker docker_build docker_push

PYCAROL_VERSION ?= $(shell grep current_version .bumpversion.cfg | sed -E 's/.*=//g;s/ //g')
TAG ?= $(PYCAROL_VERSION)

help:
	@echo "This project assumes that an active Python virtualenv is present."
	@echo "The following make targets are available:"
	@echo "	 dev 	install all deps for dev env"
	@echo "  docs	create pydocs for all relveant modules"
	@echo "	 test	run all tests with coverage"

docker_ci:
	@docker build \
		--build-arg pypi_user=$(PYPI_USERNAME) \
		--build-arg pypi_pass=$(PYPI_PASSWORD) \
		--file Dockerfile.buildkite \
		--tag pycarolci .

docker:
	@./hack/docker.sh

docker_build:
	@echo "~~~ Building Docker Image"
	@docker build \
		--build-arg PYCAROL_VERSION=$(PYCAROL_VERSION) \
		--file Dockerfile \
		--tag pycarol:$(TAG) .

docker_push:
	@echo "~~~ Pushing to Docker Hub"
	@docker tag pycarol:$(TAG) totvslabs/pycarol:$(TAG)
	@docker push totvslabs/pycarol:$(TAG)

clean:
	rm -rf dist/*

dev:
	pip install -e .

docs:
	$(MAKE) -C docs html

package:
	python setup.py sdist
	python setup.py bdist_wheel

deploy:
	twine upload dist/*.tar.gz

setup_pypi:
	echo "[pypi]" > ~/.pypirc
	echo "username = ${PYPI_USERNAME}" >> ~/.pypirc
	echo "password = ${PYPI_PASSWORD}" >> ~/.pypirc

test:
	# coverage --collect-only run -m unittest discover
	echo "This is a temporary step. CHECK THOSES TESTS"
	nosetests --with-coverage3 --collect-only

code_scan: test
	sonar-scanner \
		-Dsonar.projectKey=pyCarol \
		-Dsonar.sources=. \
		-Dsonar.host.url=https://sonar.ops.carol.ai \
		-Dsonar.login=$(SONAR_PYCAROL_TOKEN) \
		-Dsonar.branch.name=$(BUILDKITE_BRANCH)

bump_patch:
	bumpversion patch

bump_minor:
	bumpversion minor

ci: clean package setup_pypi
