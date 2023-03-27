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

ci: setup sonar

setup:
	@./hack/setup.sh

release:
	@./hack/release.sh

clean:
	@rm -rf dist/*

dev:
	@pip3 --quiet install -e .

docs:
	$(MAKE) -C docs html

package: clean
	@echo "~~~ Packaging pyCarol"
	@python3 setup.py sdist
	@python3 setup.py bdist_wheel

test_package:
	@echo "~~~ Packaging pyCarol"
	@python3 setup.py sdist
	@python3 setup.py bdist_wheel
	@twine check dist/*.tar.gz

deploy:
	@echo "~~~ Publishing pyCarol on PyPI"
	@twine upload dist/*.tar.gz --verbose

test:
	@echo "~~~ Testing pyCarol"
	# coverage --collect-only run -m unittest discover

sonar: test
	@echo "~~~ Scanning Code of pyCarol"
	@sonar-scanner \
		-Dsonar.projectKey=pyCarol \
		-Dsonar.sources=. \
		-Dsonar.host.url=https://sonar.ops.totvslabs.com \
		-Dsonar.login=$(SONAR_PYCAROL_TOKEN) \
		-Dsonar.branch.name=$(BUILDKITE_BRANCH)
