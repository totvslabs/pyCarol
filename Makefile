.PHONY: help clean dev docs package test deploy ci docker setup hub sonar

PATH := $(CURDIR)/bin:$(CURDIR)/bin/sonar-scanner/bin:$(PATH)
# TODO(amalucelli): probably there's a better way, but idk, sorry
PYCAROL_VERSION ?= $(shell grep current_version .bumpversion.cfg | sed -E 's/.*=//g;s/ //g')
TAG ?= $(PYCAROL_VERSION)

help:
	@echo "This project assumes that an active Python virtualenv is present."
	@echo "The following make targets are available:"
	@echo "	 dev 	install all deps for dev env"
	@echo "  docs	create pydocs for all relveant modules"
	@echo "	 test	run all tests with coverage"

ci: setup clean package sonar

setup:
	@./hack/setup.sh

release:
	@./hack/release.sh

docker:
	@echo "~~~ Building Docker Image"
	@docker build \
		--build-arg PYCAROL_VERSION=$(PYCAROL_VERSION) \
		--file Dockerfile \
		--tag pycarol:$(TAG) .

hub:
	@echo "~~~ Pushing to Docker Hub"
	@docker tag pycarol:$(TAG) totvslabs/pycarol:$(TAG)
	@docker push totvslabs/pycarol:$(TAG)

clean:
	@rm -rf dist/*

dev:
	pip3 install -e .

docs:
	$(MAKE) -C docs html

package:
	python3 setup.py sdist
	python3 setup.py bdist_wheel

deploy:
	twine upload dist/*.tar.gz

test:
	# coverage --collect-only run -m unittest discover
	echo "This is a temporary step. CHECK THOSES TESTS"
	nosetests --with-coverage3 --collect-only

sonar: test
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
