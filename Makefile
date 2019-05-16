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
	twine upload dist/*.tar.gz -r totvslabs

setup_pypi:
	echo "[distutils]" > ~/.pypirc
	echo "index-servers =" >> ~/.pypirc
	echo "    totvslabs" >> ~/.pypirc
	echo "" >> ~/.pypirc
	echo "[totvslabs]" >> ~/.pypirc
	echo "repository: http://nexus3.carol.ai:8080/repository/labspypi/" >> ~/.pypirc
	echo "username = ${PYPI_USERNAME}" >> ~/.pypirc
	echo "password = ${PYPI_PASSWORD}" >> ~/.pypirc

setup_pip:
	pip config set global.index http://nexus3.carol.ai:8080/repository/totvslabspypi/pypi
	pip config set global.index-url http://nexus3.carol.ai:8080/repository/totvslabspypi/simple
	pip config set global.trusted-host nexus3.carol.ai

test:
	# coverage --collect-only run -m unittest discover
	echo "This is a temporary step. CHECK THOSES TESTS"
	nosetests --with-coverage3 --collect-only

ci: clean test package setup_pypi deploy
