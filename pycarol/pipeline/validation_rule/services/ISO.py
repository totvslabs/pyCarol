import xmltodict
import os
from ..uniform_data_rule import ExternalServiceInterface
from ..uniform_data_rule import DATA_STORAGE as BASE_DATA_STORAGE

DATA_STORAGE = os.path.join(BASE_DATA_STORAGE, 'iso')


class IsoSrvc(ExternalServiceInterface):
    """ Interface to interpret ISO XML values

    """

    @classmethod
    def get_from(cls, iso_num, field, table, **options):
        """ Get ISO information from XML file

            ISO: ISO number
            table: Table where ISO values are located
            field: Field that have ISO values
        """
        doc = cls._get_XML_from_iso(iso_num)
        return cls._to_mdmuniform_shape(doc, iso_num, field, table)

    @staticmethod
    def _get_XML_from_iso(iso_num: str):
        """ Access file or database and get corresponding XML

            iso_num: ISO number
        """
        filename = os.path.join(DATA_STORAGE, 'iso' + iso_num + '.xml')
        with open(filename) as fd:
            doc = xmltodict.parse(fd.read())
        _, doc = doc.popitem()
        return doc

    @staticmethod
    def _to_mdmuniform_shape(doc, iso_num, field, table):
        """ Convert dict of ISO into MdmUniform format

            table: Str with table name
            field: Name of field

            Return:
                List of values for specified table and field

            E.g.:

                _to_mdm_uniform_shape(doc, 'ISONUM', 'Table1', 'Field1')
                Would output:
                    {
                        'ISO_ISONUM_Field1':[]
                    }
        """
        ret = []
        items = list(list(doc.items())[1][1].items())
        for i in range(len(items)):
            table_name = items[i][0]

            if table_name == table:
                table_values = items[i][1]
                for i in table_values:
                    if field in i:
                        ret.append(i[field])
        return {f'ISO_{iso_num}': ret}