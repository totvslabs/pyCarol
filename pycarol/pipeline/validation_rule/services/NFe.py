import pandas as pd
import os
from ..uniform_data_rule import ExternalServiceInterface
from ..uniform_data_rule import DATA_STORAGE as BASE_DATA_STORAGE

DATA_STORAGE = os.path.join(BASE_DATA_STORAGE, 'nfe')


class NfeSrvc(ExternalServiceInterface):

    @classmethod
    def get_from(cls, std, field, table=None, **options):
        file, options = cls._get_std_file(std)
        values = cls.read_table(file, field, **options)
        return {f'{std}': values}

    @classmethod
    def _get_std_file(cls, std):
        # TODO Improve organization
        return {
            'ncm': (
                'TABELA_NCM_2018-A.xlsx', dict(
                    skiprows=[0])),
            'unidade_comercial': (
                'Tabela_Unidades_de_Medida_Comercial_05072016.xls', dict(
                    skiprows=[0, 1]))
        }[std]

    @classmethod
    def read_table(cls, filename, field, table=None, sheet_num=0, header=[0], skiprows=None):
        """
         Returns a list with field from table
        """
        # TODO Other file extension
        # TODO More complicated types of tables
        df = pd.read_excel(os.path.join(DATA_STORAGE, filename), header=header, skiprows=skiprows)
        return df[field].values.tolist()
