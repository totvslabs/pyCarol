# Architecture

## Run
Each project must have a run.py file to be executed.

## Luigi

Pipelines should be organized using luigi and/or luigi-extension

### Ingestion

- Tasks must end with 'Ingestion'

### Validation

- Tasks must end with 'Validation'

#### DataModel

- Tasks must have 'DataModel' before 'Validation'. E.g. MydatamodelDataModelValidation

#### Model

- Tasks must have 'Model' before 'Validation'. E.g. MymodelModelValidation

