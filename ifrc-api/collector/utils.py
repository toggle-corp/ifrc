from collections import defaultdict
from .country import (
    get_country_iso2,
    in_countries_iso3,
    in_countries_iso2,
)


def get_dict(data, fields, split='__', default=''):
    _data = data
    fields = fields.split(split)
    for field in fields:
        if not isinstance(_data, dict):
            return _data
        _data = _data.get(field)
        if _data is None:
            return default
    return _data


def dataframe_to_nested_dict(grouped):
    nested = {}
    results = defaultdict(lambda: defaultdict(dict))
    for index, value in grouped.itertuples():
        for i, key in enumerate(index):
            if i == 0:
                nested = results[key]
            elif i == len(index) - 1:
                nested[key] = value
            else:
                nested = nested[key]
    return results


def add_country_meta(data, field, iso2):
    """
    data: add country iso to data
    field: field which contains iso2/iso3
    iso2: field points to iso2
    """
    if iso2:
        data['country'] = get_dict(data, field)
    else:
        data['country'] = get_country_iso2(get_dict(data, field))
    return data


def filter_data(data, field, iso2):
    """
    Only keep for country which we have
    data: main data
    field: field which contains iso2/iso3
    iso2: field points to iso2
    """
    _data = []
    in_countries_iso = in_countries_iso2 if iso2 else in_countries_iso3
    for datum in data:
        if in_countries_iso(get_dict(datum, field)):
            _data.append(datum)
    return _data
