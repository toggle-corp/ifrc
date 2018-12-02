import pycountry
import requests
import json
import logging

from .common import gen_output_path, dump_json_to_file, load_json_from_file

logger = logging.getLogger(__name__)

COUNTRY_ISO3_API = 'http://country.io/iso3.json'
COUNTRY_API = 'http://dsgocdnapi.azureedge.net/api/v2/country/?limit=300'


"""
iso2 -> iso3 {'BD': 'BGD', 'BE': 'BEL', 'BF': 'BFA', 'BG': 'BGR', ...}
iso3 -> iso2 {'BGD': 'BD', 'BEL': 'BE', 'BFA': 'BF', 'BGR': 'BG', ...}
"""
country_iso3 = requests.get(COUNTRY_ISO3_API).json()
country_iso3['AN'] = 'ANT'
country_iso3['CS'] = 'SCG'

country_iso2 = {country_iso3[iso3]: iso3 for iso3 in country_iso3}

missed_country = {
    'cape verde': 'CPV',
    'syria': 'SYR',
    'gaza strip': 'GAZ',
    'north korea': 'PRK',
    'netherlands antilles': 'ANT',
    # 'south korea': 'KOR',
}


def get_country_from_name(name):
    try:
        return pycountry.countries.get(name=name)
    except KeyError:
        try:
            return pycountry.countries.lookup(name)
        except LookupError:
            return pycountry.countries.get(
                alpha_3=missed_country[name.lower()]
            )


def get_country_iso2(iso='', name=None):
    if name:
        try:
            return get_country_from_name(name).alpha_2
        except KeyError:
            return country_iso3.get(missed_country[name.lower()])
    if iso and len(iso) == 3:
        return country_iso2.get(iso.upper())
        # return pycountry.countries.get(alpha_3=iso.upper()).alpha_2
    return iso
    # return pycountry.countries.get(alpha_3=iso.upper()).alpha_2


def get_country_iso3(iso='', name=None):
    if name:
        try:
            return get_country_from_name(name).alpha_3
        except KeyError:
            return missed_country[name.lower()]
    if iso and len(iso) == 2:
        return country_iso3.get(iso.upper())
        # return pycountry.countries.get(alpha_2=iso.upper()).alpha_3
    return iso
    # return pycountry.countries.get(alpha_2=iso.upper()).alpha_3


def map_data(country_info):
    country_info['iso'] = country_info.get('iso', '').upper()
    country_info['iso3'] = get_country_iso3(country_info['iso']).upper()
    return country_info


class CountryApi():

    def __init__(self, path=None):
        self.countries_isos = []
        self.countries = []
        self.load(path)

    def load(self, path=None):
        try:
            self.countries = load_json_from_file(path)
            print('Using Local Country Data')
        except (TypeError, FileNotFoundError, json.decoder.JSONDecodeError):
            self.pull()
            if path is not None:
                self.save(path)
        return self.countries

    def save(self, path):
        dump_json_to_file(path, self.countries)

    def pull(self):
        print('Pulling Country Data')
        countries = []
        response = requests.get(COUNTRY_API)
        data = response.json().get('results', [])
        for country_data in data:
            country = map_data(country_data)
            if country.get('iso'):
                countries.append(country)
        self.countries = countries

    def get_countries_iso(self, iso2=False):
        iso = 'iso' if iso2 else 'iso3'
        return [country.get(iso) for country in self.countries]

    def get_countries_id(self):
        return [country.get('id') for country in self.countries]

    def get_countries(self):
        return self.countries


if __name__ == '__main__':
    countries = CountryApi(gen_output_path('country.json'))
    print(json.dumps(countries))
else:
    country_api = CountryApi(gen_output_path('country.json'))
    countries = country_api.get_countries()
    countries_iso2 = country_api.get_countries_iso(iso2=True)
    countries_iso3 = country_api.get_countries_iso()

    def in_countries_iso2(iso2=''):
        return iso2.upper() in countries_iso2

    def in_countries_iso3(iso3=''):
        return iso3.upper() in countries_iso3
