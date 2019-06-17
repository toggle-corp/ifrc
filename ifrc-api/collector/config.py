class NotCacheException(Exception):
    message = 'Don\' use cache'


class settings():
    output_dir = '.cache'


class Units():
    count = 'count'
    average = 'average'


class Sources():
    reliefWeb = 'reliefweb'
    acled = 'acled'
    go_api = 'prddsgocdnapi.azureedge.net'


class Strings():
    units = Units
    sources = Sources


class Fields():
    value = 'value'
    count = 'count'
    source_url = 'sourceUrl'
    source = 'source'
    date_pulled = 'datePulled'
    unit = 'unit'


class GoApiFields():
    amount_requested = 'amountRequested'
    amount_funded = 'amountFunded'
    num_beneficiaries = 'numBeneficiaries'
    name = 'name'
    value = 'value'


class ReliefFields():
    id = 'id'
    source_url = 'sourceUrl'
    name = 'name'
    glide = 'glide'
    ongoing = 'ongoint'
    disaster_url = 'disasterUrl'
    description = 'description'
    date = 'date'
    num_countries = 'numCountries'
    primary_type_code = 'primaryTypeCode'
    primary_type_name = 'primaryTypeName'
    primary_type_name_ifrc_tax = 'primaryTypeNameIfrcTax'
