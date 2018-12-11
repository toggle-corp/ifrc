import json
import requests
import traceback
from os.path import join as path_join
from dateutil import parser as datetime_parser

from .config import GoApiFields as _ts, NotTestException
from .common import (
    dump_json_to_file, load_json_from_file,
    get_year_month_formatted, gen_output_path,
    load_pickle_from_file, dump_pickle_to_file, snakecase_to_camelCase
)
# from utils import add_country_meta

"""
https://prddsgocdnapi.azureedge.net/docs/

atype:

"""
API_ENDPOINT = 'https://prddsgocdnapi.azureedge.net/api/v2'
APPEL_URL = API_ENDPOINT + '/appeal/'
DISASTER_TYPE_URL = API_ENDPOINT + '/disaster_type/'

ATYPE = {
    0: 'DREF',
    1: 'APPEAL',
    2: 'INTL',
}

RegionName = {
    0: 'AFRICA',
    1: 'AMERICAS',
    2: 'ASIA_PACIFIC',
    3: 'EUROPE',
    4: 'MENA',
}

Regions_Id = RegionName.keys()

POP_FIELDS = [
    'created_at', 'modified_at', 'event', 'needs_confirmation', 'status',
    'aid', 'region', 'country',
]


def normalize(data):
    # data = add_country_meta(data, 'country__iso', iso2=True)
    for field in POP_FIELDS:
        try:
            data.pop(field)
        except KeyError:
            pass
    _data = {}
    for key in data.keys():
        _data[snakecase_to_camelCase(key)] = data[key]
    _data['atype'] = ATYPE[data.get('atype')]
    return _data


def get_disaster_types():
    response = requests.get(DISASTER_TYPE_URL)
    return {
        disaster['id']: disaster['name']
        for disaster in response.json()['results']
    }


def get_disasters_id():
    response = requests.get(DISASTER_TYPE_URL)
    results = response.json().get('results')
    return [disasters.get('id') for disasters in results]


def go_api_appeal_full_data(params={}):
    params['limit'] = 1
    response = requests.get(APPEL_URL, params=params)
    params['limit'] = response.json().get('count')
    response = requests.get(APPEL_URL, params=params)
    return response.json()


def go_api_appeal(params={}):
    response = requests.get(APPEL_URL, params=params)
    return response.json()


class GoApi():
    DATA_FILENAME = 'data.json'
    SUMMARY_FILENAME = 'summary.json'

    def __init__(self, path, test=False):
        self.data = None
        # TODO: also calculate for region wise
        self.summary = None
        self.summary = None

        self.data_filename = path_join(path, GoApi.DATA_FILENAME)
        self.summary_filename = path_join(path, GoApi.SUMMARY_FILENAME)

        try:
            if not test:
                raise NotTestException()
            self.data = load_json_from_file(self.data_filename)
            self.summary = load_pickle_from_file(self.summary_filename)
            print('Using Local Go Api Data')
        except (
                TypeError, FileNotFoundError, json.decoder.JSONDecodeError,
                NotTestException,
        )\
                as e:
            self.load_data(path)

    def load_data(self, path):
        if not self.data:
            print('Pulling Go Api Data')
            response = requests.get(APPEL_URL)
            params = {'limit': response.json().get('count')}
            response = requests.get(APPEL_URL, params)
            self.data = response.json().get('results', [])
            dump_json_to_file(self.data_filename, self.data)
        print('Re-calculating Go Api Data')
        self.summary = GoApi.get_summary(self.data)
        dump_pickle_to_file(self.summary_filename, self.summary)

    @staticmethod
    def get_summary(data):
        summary_cw = {}
        summary_rw = {}
        disaster_types = get_disaster_types()
        for summary in [summary_cw, summary_rw]:
            for datum in data:
                country_detail = datum.get('country')
                dtype = int(datum['dtype']['id'])
                atype = ATYPE[int(datum.get('atype'))]
                date = datetime_parser.parse(datum.get('start_date'))
                num_beneficiaries = datum.get('num_beneficiaries')
                amount_requested = float(datum.get('amount_requested'))
                amount_funded = float(datum.get('amount_funded'))
                date_formated = get_year_month_formatted(date=date)

                if summary is summary_cw:
                    # Skip if country is not provided or data are zero
                    if not country_detail or (
                        amount_funded == 0 and amount_requested == 0 and
                            num_beneficiaries == 0
                    ):
                        continue
                    country = int(country_detail['id'])
                else:
                    country = datum.get('region', {}).get('id')
                    # Skip if region is not provided
                    if country is None:
                        continue

                response_data = {
                    _ts.amount_requested: amount_requested,
                    _ts.amount_funded: amount_funded,
                    _ts.num_beneficiaries: num_beneficiaries,
                }

                country_data = summary.get(country)
                if country_data:
                    dtype_data = country_data.get(dtype)
                    if dtype_data:
                        atype_data = dtype_data[_ts.value].get(atype)
                        if atype_data:
                            month_data = atype_data.get(date_formated)
                            if month_data:
                                month_data[_ts.amount_requested] +=\
                                    amount_requested
                                month_data[_ts.amount_funded] += amount_funded
                                month_data[_ts.num_beneficiaries] +=\
                                    num_beneficiaries
                            else:
                                atype_data[date_formated] = response_data
                        else:
                            dtype_data[_ts.value][atype] = {
                                date_formated: response_data
                            }
                    else:
                        country_data[dtype] = {
                            _ts.name: disaster_types.get(dtype),
                            _ts.value: {
                                atype: {date_formated: response_data}
                            }
                        }
                else:
                    summary[country] = {
                        dtype: {
                            _ts.name: disaster_types.get(dtype),
                            _ts.value: {atype: {date_formated: response_data}},
                        }
                    }
        return {
            'cw': summary_cw,
            'rw': summary_rw
        }

    @staticmethod
    def latest_operation_appeal_DREF_with_budget_and_targeted_beneficiaries(
            country_id
    ):
        """
        Latest Operation (Appeal/DREF) with budget and targeted beneficiaries
        """
        params = {'limit': 1, 'ordering': 'id', 'country': country_id}
        data = {}
        try:
            results = go_api_appeal(params).get('results', [])
            if len(results) > 0:
                data = normalize(results[0])
        except json.decoder.JSONDecodeError:
            traceback.print_exc()
        return APPEL_URL, data

    def num_of_op_that_IFRC_launched_to_by_type(
            self, country_id=None, region_id=None
    ):
        """
        Number of Operations that IFRC Launched to (by type)
        * group by:
            dtype -> atype (0: appeal, 1: DREF) -> yearly -> monthly
        * Using start date field for date
        * Both countrywise and also regionwise
        * include sum of:
            amount_requested, amount_funded, num beneficiaries
        """
        if country_id is not None:
            return self.summary['cw'].get(country_id, [])
        elif region_id is not None:
            return self.summary['rw'].get(region_id, [])


if __name__ == '__main__':
    """
    print(json.dumps({
        # 14 is AFG
        'detail': 'Latest Operation (Appeal/DREF) with budget'
        ' and targeted beneficiaries',
        'data':
        latest_operation_appeal_DREF_with_budget_and_targeted_beneficiaries(14)
    }))
    """
    goApi = GoApi(gen_output_path('go_api'))

    region = 1
    print(json.dumps({
        # 14 is AFG
        'detail': 'Number of Operations that IFRC Launched to (by type)',
        'AFG':
        goApi.num_of_op_that_IFRC_launched_to_by_type(country_id=14),
        RegionName[region]:
        goApi.num_of_op_that_IFRC_launched_to_by_type(region_id=region)
    }))
