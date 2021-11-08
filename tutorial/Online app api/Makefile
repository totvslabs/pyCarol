PYLINT = flake8
PYLINTFLAGS = -rn
PYTHONFILES := $(wildcard *.py)
SRCFOLDER = app

ifeq ($(OS),Windows_NT)
	OPEN := start
else
	UNAME := $(shell uname -s)
	ifeq ($(UNAME),Linux)
		OPEN := xdg-open
	endif
	ifeq ($(UNAME),Darwin)
		OPEN := open
	endif
endif

.PHONY: help

help: ## Show this help message
	@echo "sentence_similarity_api Makefile help.\n"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

init: ## Install all package dependencies
	pip install -r requirements.txt

run: ## Runs Api locally on http://localhost:5000
	PYTHONPATH=. PYTHONUNBUFFERED=1 FLASK_DEBUG=1 FLASK_ENV=development python main.py

docker_image: ## Build Carol app docker image
	@docker build -t sentence_similarity_api .

docker_run: ## Runs Carol app docker image
	@docker run --rm -it -p 5000:5000 --env-file ./prod/.env sentence_similarity_api

docker_run_homolog: ## Runs Carol app docker image
	@docker run --rm -it -p 5000:5000 --env-file ./homolog/.env sentence_similarity_api