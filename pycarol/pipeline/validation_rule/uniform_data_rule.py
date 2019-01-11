import itertools
import os
import json
import numpy as np

from .validation_rule import ValidationRule


DATA_STORAGE = os.path.join(os.path.dirname(__file__), 'service', 'data')
RULES_STORAGE = os.path.join(os.path.dirname(__file__), 'rules')


class MdmUniformType:
    """ Data type for uniformity on MDM Fields

    """

    def __init__(self, name, description='', version='1.0.0', is_global=True, values=None, formats=None, id=None,
                 **kwargs):
        """
            name: Identifier label
            last_update: date
            description: Description of rule
            additional_info: Dict with additional information
            version: Version for the type
            is_global: Whether is a global type or not
            values: dict of values (list)
                We chose to have a dict of lists, instead of only a list
                because it gives the freedom to compose rules from different
                sources.
        """

        if id is None:
            id = self._generate_id(name)
        self.id = id
        self.name = name
        self.description = description
        self.is_global = is_global
        self.values = values if values is not None else {}
        self.formats = formats if formats is not None else {}

    @staticmethod
    def _generate_id(name):
        """ Generate a unique id for the rule
        """
        # TODO: Define a way to generate ids
        return name + 'Id'

    def post(self):
        """ Store rule

            TODO: Adapt to Carol Infrastructure
        """
        rule_dict = self.to_json()
        filename = self.id + '.json'
        filename = os.path.join(RULES_STORAGE, filename)
        with open(filename, 'w') as outfile:
            json.dump(rule_dict, outfile)
        return self

    @staticmethod
    def from_id(id):
        """ Return object with the provided id

        :param id:
        :return:
        """
        # TODO
        pass

    def get_id(self):
        return self.id

    @staticmethod
    def get_rule(id, tenant_id=None, auth=None):
        """ Get rule by id

        If tenant is None, rule is global

        :param id: Rule id
        :param tenant_id: If tenant_id is None, rule is global, otherwise get rule from tenant
        :param auth: Authentication to access tenant, if applicable
        :return: MdmUniformType rule
        """
        # TODO: Change that to a service of rules, or other thing
        # List rules storage
        try:
            filename = id+'.json'
            with open(os.path.join(RULES_STORAGE, filename), 'r') as fp:
                dic = json.load(fp)
                return MdmUniformType(**dic)
        except OSError:
            raise ValueError(f'Could not open rule with ID = {id}')

    def add_values(self, values):
        # TODO Check values format
        self.values.update(values)

    def get_values_from(self, source, field, service=None, table=None, **options):
        """ Get rule values from Carol Rules

            source: Source Service or string source name (JSON Filename)
            field: field to get values
            table: if values are organized in tables
            service: If data comes from external services
        """
        if service is None:
            filename = os.path.join(DATA_STORAGE, source + '.json')
            with open(filename) as fd:
                json_dict = json.load(fd)
                self.values.update({f'{source}_{field}': json_dict[field]})
        else:
            values = service.get_from(source, field, table, **options)
            self.values.update(values)
        return self

    def get_all_values(self):
        return list(itertools.chain.from_iterable(self.values.values()))

    def create_regex(self):
        """ Create a regex rule from Mdm

            TEMP: https://trello.com/c/FUEpRgLh/198-data-model-enum-field-type
        """
        rule_values = list(self.get_all_values())
        rule_values = [(str(r), r)[isinstance(r, str)] for r in rule_values]
        regex = '^(?!' + '$|'.join(rule_values) + '$).+'
        return regex

    def to_json(self):
        # TODO
        return self.__dict__

    def to_validation_rule(self):
        params = self.create_regex()
        return ValidationRule(ValidationRule.Operation.MATCHES, [params])

    def validate(self, data, ignore_errors = True):
        allowed_vals = self.get_all_values()

        # Ignore null data
        data = filter(lambda v: v==v, data)

        # Success Message
        msg = self.name + ' - ' + self.description
        valid = np.array([d in allowed_vals for d in data])
        success = all(valid)
        if not success:
            invalid_data = data[~valid]
            msg = 'Wrong data: ' + str(invalid_data[:min(100, len(invalid_data))])
        return success, {'success': success, 'msg': msg}


class ExternalServiceInterface:
    """ TODO: Interface to deal with external services data sources
    """

    @classmethod
    def get_from(cls, service_name, field, table=None, **options):
        pass

    @classmethod
    def update_values(cls, service_name):
        pass
