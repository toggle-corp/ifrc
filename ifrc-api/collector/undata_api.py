import requests
import json
import logging

"""
Ref: https://github.com/sdmx-twg/sdmx-rest/wiki/Data-queries
"""
logger = logging.getLogger(__name__)

DEFAULT_HEADERS = {'Accept': 'text/json'}
UNDATA_API = 'http://data.un.org/ws/rest/{flowRef}/{key}/{providerRef}?{query}'


def clean_array(array):
    _array = [element for element in array if element is not None]
    return _array


def pull_data(url, data_collector=[]):
    response = requests.get(
        url, headers=DEFAULT_HEADERS
    )
    return response.text


class UnDataApi():

    @staticmethod
    def get_data(flowRef, key='', providerRef='', query=''):
        data = {
            'flowRef': flowRef,
            'key': key,
            'providerRef': providerRef,
            'query': query,
        }
        url = UNDATA_API.format(**data)
        data = pull_data(url)
        return data


if __name__ == '__main__':
    data = UnDataApi.get_data(
        'data', 'DF_UNData_UNFCC', 'A.EN_ATM_PFCE.DNK.Gg_CO2'
    )
    print(json.dumps(data))
