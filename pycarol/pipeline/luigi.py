from luigi_extension import Task, inherit_list
from luigi import Parameter


class DataModelValidation(Task):
    domain = Parameter(default='Unspecified')
    ignore_errors = Parameter(default=True)

    def easy_run(self, inputs):
        log = {'domain': self.domain,
               'datamodel': self.dm.get_name()}

        success, log_dm = self.dm.validate(inputs[0], ignore_errors=self.ignore_errors)

        if not self.ignore_errors and not success:
            raise ValueError(log)
        log['log'] = log_dm
        return log

# Create tasks that generate reports/VISUALIZATIONS! :D

# Create Luigi mappings

# Create tasks from Data Models information automatically

# Create Ingestion tasks
