import requests
import json
import logging

from .country import get_country_iso3
from .config import ReliefFields as _ts
from .common import (
    get_months_from_years, get_iso_month_start_end_day,
    get_year_month_formatted, async_post, normal_to_camel_case
)

logger = logging.getLogger(__name__)


def pull_data(url, payload, data_collector=[]):
    response = requests.post(url, data=json.dumps(payload))
    data = json.loads(response.text).get('data')
    next_url = json.loads(response.text).\
        get('links', {}).get('next', {}).get('href')

    if data is not None:
        data_collector.extend(data)
    else:
        print('-' * 22)
        logger.error(
            'url: %s\npayload: %s\nresponse: %s',
            url, payload, response.text
        )
        print('-' * 22)
    if next_url:
        return pull_data(next_url, payload, data_collector)
    return data_collector


RELIEFWEB_API = 'https://api.reliefweb.int/v1'
R_DISASTERS_URL = RELIEFWEB_API + '/disasters?appname=ifrc-go'
R_DISASTER_URL = RELIEFWEB_API + '/disasters/{}?appname=ifrc-go'
R_REPORTS_URL = RELIEFWEB_API + '/reports?appname=ifrc-go'
PRIMARY_COUNTRY_ISO3_FIELDNAME = 'primary_country.iso3'

DISASTER_TRANSLATE_IFRC = {
    'CW': 'Cold Wave',
    'HT': 'Heat Wave',
    'DR': 'Drought',
    'EQ': 'Earthquake',
    'LS': 'Land Slide',
    'TS': 'Tsunami',
    'VO': 'Volcano',
    'EC': 'Extratropical Cyclone',
    'TC': 'Tropical Cyclone',
    'SS': 'Storm Surge',
    'FL': 'Flood',
    'FF': 'Flash Flood',
    'CE': 'Complex Emergency',
    'FR': 'Fire',
    'OT': 'Other',
}
"""
http://apidoc.rwlabs.org/
https://api.reliefweb.int/v1/references/disaster-types
https://api.reliefweb.int/v1/disasters/
"""


def normalize_disaster(response):
    json_response = response.json()
    url = json_response['links']['self']['href']
    disaster = json_response['data'][0]['fields']
    return {
        _ts.source_url: url,

        _ts.id: disaster['id'],
        _ts.name: disaster['name'],
        _ts.glide: disaster['glide'],
        _ts.ongoing: disaster['current'],
        _ts.disaster_url: disaster['url'],
        _ts.description: disaster['description'],

        _ts.date: disaster['date']['created'],
        _ts.num_countries: len(disaster['country']),
        _ts.primary_type_code: disaster['primary_type']['code'],
        _ts.primary_type_name: disaster['primary_type']['name'],
        _ts.primary_type_name_ifrc_tax:
            DISASTER_TRANSLATE_IFRC[disaster['primary_type']['code']],
    }


class ReliefWebApi():

    @staticmethod
    def get_reports(payload):
        data = pull_data(R_REPORTS_URL, payload)
        return data

    @staticmethod
    def get_disasters(payload):
        data = pull_data(R_DISASTERS_URL, payload)
        return data

    @staticmethod
    def get_latest_disaster(country_iso):
        response = requests.get(R_DISASTERS_URL, params={
            PRIMARY_COUNTRY_ISO3_FIELDNAME: country_iso,
            'limit': 1,
            'sort': 'date:desc',
        })
        disasters = response.json().get('data', [])
        if len(disasters) > 0:
            disaster_id = disasters[0]['id']
            disaster = normalize_disaster(
                requests.get(R_DISASTER_URL.format(disaster_id))
            )
            return disaster

    @staticmethod
    def get_count_of_reported_events_filtered_10y_pull(iso, names, date_range):
        date_range = get_months_from_years(10)
        urls_with_params = []
        for _name in names:
            for date in date_range:
                from_d, to_d = get_iso_month_start_end_day(**date, isoformat=True)
                name = names if _name == 'others' else _name
                negate = True if _name == 'others' else False
                params = {
                    'limit': 1,
                    'filter': {
                        'operator': 'AND',
                        'conditions': [
                            {
                                'field': 'name',
                                'value': name,
                                'negate': negate,
                            },
                            {
                                'field': 'date.created',
                                'value': {
                                    'from': from_d,
                                    'to': to_d,
                                }
                            },
                            {
                                'field': PRIMARY_COUNTRY_ISO3_FIELDNAME,
                                'value': get_country_iso3(iso).lower(),
                            },
                        ],
                    },
                }
                urls_with_params.append((
                    R_DISASTERS_URL, params, {
                        'name': _name,
                        'date': date,
                    }
                ))
        response = async_post(urls_with_params, limit_per_host=5)
        return response

        """
        return await asyncio.gather(*tasks)
        response = requests.post(R_DISASTERS_URL, data=json.dumps({}))
        return [
            response.json().get('totalCount'), date
        ]
        """

    @staticmethod
    def get_count_of_reported_events_filtered_10y(
            country_iso,
            names=[
                'Cholera outbreak', 'Meningitis', 'Rift Valley fever',
                'Viral haemorrhagic fevers', 'Viral hepatitis A B C E',
                'Yellow fever', 'others',
            ],
    ):
        # TODO: Send hepatitis as a array
        """
        # of reported events by month (last 10 years average)
        - Cholera outbreak
        - Meningitis
        - Rift Valley fever
        - Viral haemorrhagic fevers
        - Viral hepatitis (A, B, C, E)
        - Yellow fever
        - Others...
        """
        collector = {}
        date_range = get_months_from_years(10)
        responses = ReliefWebApi.get_count_of_reported_events_filtered_10y_pull(
            country_iso, names, date_range
        )
        for response, params in responses:
            date = params['date']
            name = normal_to_camel_case(params['name'])
            count = response['json'].get('totalCount')
            date_formated = get_year_month_formatted(**date)
            # Skip is count is none or 0
            if not count:
                if not collector.get(name):
                    collector[name] = {}
                continue
            if collector.get(name):
                collector[name][date_formated] = count
            else:
                collector[name] = {date_formated: count}
        return R_DISASTERS_URL, collector

    @staticmethod
    def get_count_of_reported_events_10y(country_iso):
        """
        # of reported events (last 10 years average)
        """
        response = requests.post(R_DISASTERS_URL, data=json.dumps({
            'limit': 1,
            'filter': {
                'operator': 'AND',
                'conditions': [
                    {
                        'field': 'date.created',
                        'value': {
                            'from': '2008-01-01T00:00:00+00:00',
                        }
                    },
                    {
                        'field': PRIMARY_COUNTRY_ISO3_FIELDNAME,
                        'value': get_country_iso3(country_iso).lower(),
                    },
                ],
            },
        }))
        return response.url, response.json().get('totalCount')


if __name__ == '__main__':
    disaster = [
        'Cholera outbreak', 'Meningitis', 'Rift Valley fever',
        'Viral haemorrhagic fevers', 'Viral hepatitis (A, B, C, E)',
        'Yellow fever',
    ]
    time = {
        'from': '2004-06-01T00:00:00+00:00',
        'to': '2018-06-30T23:59:59+00:00',
    }

    payload = {
        'filter': {
            'operator': 'AND',
            'conditions': [
                {
                    'field': 'name',
                    'value': disaster,
                },
                {
                    'field': 'date.created',
                    'value':
                    {
                        'from': time['from'],
                        'to': time['to'],
                    }
                },
            ],
        },
        'fields': {
            'include': ['name', 'type'],
        }
    }

    data = ReliefWebApi.get_disasters_data(payload)
    print(json.dumps(data))

    print('--' * 22)

    data = ReliefWebApi.get_reports(payload)
    print(json.dumps(data))
