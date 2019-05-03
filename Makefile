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
	pip install -r dev-requirements.txt
	pip install -e .

docs:
	$(MAKE) -C docs html

package:
	python setup.py sdist
	python setup.py bdist_wheel

deploy:
	twine upload dist/* -r totvslabs

setup_pypi:
	echo -e "[distutils]" > ~/.pypirc
	echo -e "index-servers =" >> ~/.pypirc
	echo -e "	totvslabs" >> ~/.pypirc
	echo -e "[totvslabs]" >> ~/.pypirc
	echo -e "repository = http://nexus3.carol.ai:8080/repository/totvslabspypi/pypi" >> ~/.pypirc
	echo -e "username = ${PYPI_USERNAME}" >> ~/.pypirc
	echo -e "password = ${PYPI_PASSWORD}" >> ~/.pypirc
	echo -e "trusted-host = nexus3.carol.ai"
	pip config set global.index http://nexus3.carol.ai:8080/repository/totvslabspypi/pypi
	pip config set global.index-url http://nexus3.carol.ai:8080/repository/totvslabspypi/simple
	pip config set global.trusted-host nexus3.carol.ai

test:
	# coverage --collect-only run -m unittest discover
	echo "This is a temporary step. CHECK THOSES TESTS"
	nosetests --with-coverage3 --collect-only
	coverage xml -o cov.xml

ci: clean package setup_pypi deploy