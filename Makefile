.PHONY: help clean dev docs package test deploy setup_pypi ci

help:
	@echo "This project assumes that an active Python virtualenv is present."
	@echo "The following make targets are available:"
	@echo "	 dev 	install all deps for dev env"
	@echo "  docs	create pydocs for all relveant modules"
	@echo "	 test	run all tests with coverage"

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

bump_patch:
	bumpversion patch

bump_minor
	bumpversion minor

ci: clean test package setup_pypi deploy
