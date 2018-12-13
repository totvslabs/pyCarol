# TEMP: This task should be placed somewhere else
from ..uniform_data_rule import MdmUniformType
from ..services.ISO import IsoSrvc
from ..services.NFe import NfeSrvc


def generate_rules():
    """ Create new uniform rules that are used in the Supply Chain Data models
    OBS! *************
    This method is intended to be temporary, since data model uniform rules should be created on the Carol platform
    Check MDMUniformType for more details.
    """
    # Currency: Generate from ISO 4217 and table Cryptocurrencies
    currency_rule = MdmUniformType(name='currency', description='Default Format for Currencies')
    currency_rule.get_values_from('4217', 'Ccy', table='CcyNtry', service=IsoSrvc).post()

    # Commercial Unity

    # NCM
    ncm_rule = MdmUniformType(name='ncm', description='NCM Identifiers')
    ncm_rule.get_values_from('ncm', 'NCM', service=NfeSrvc).post()

    # Purchase Status
    MdmUniformType(name='purchase_status', description='Status of a purchase',
                   values={'custom':['printed', 'open', 'cancelled', 'delivered']}).post()

    # Purchase Type
    MdmUniformType(name='purchase_type', description='Type of purchase',
                   values={'custom':['invoice', 'order', 'other']})

    # Government Id Type
    MdmUniformType(name='government_id', description='Government ID',
                   values=['invoice', 'order', 'other'])

