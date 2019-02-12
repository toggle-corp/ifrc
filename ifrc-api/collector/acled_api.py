import json
import requests
import logging
from os.path import join as path_join

import pandas

from .country import get_country_iso3
from .config import NotTestException
from .utils import dataframe_to_nested_dict
from .common import (
    load_json_from_file, dump_json_to_file, get_file_created_iso_date,
    normal_to_camel_case
)

logger = logging.getLogger(__name__)

"""
Ref: https://www.acleddata.com/wp-content/uploads/2013/12/API-User-Guide-August-2017.pdf  # noqa
"""

# ACLED_API_RAW = 'https://api.acleddata.com/acled/read'
ACLED_API = 'https://api.acleddata.com/acled/read?limit=0&terms=accept'


class AcledApi():
    DATA_FILENAME = 'data.json'
    SUMMARY_FILENAME = 'summary.json'

    def __init__(self, path, test=False):
        self.data = None
        self.summary = None

        self.data_filename = path_join(path, AcledApi.DATA_FILENAME)
        self.summary_filename = path_join(path, AcledApi.SUMMARY_FILENAME)

        try:
            if not test:
                raise NotTestException
            self.data = load_json_from_file(self.data_filename)
            self.summary = load_json_from_file(self.summary_filename)
            print('Using Local Acled Data')
        except (
                TypeError, FileNotFoundError, json.decoder.JSONDecodeError,
                NotTestException,
        )\
                as e:
            self.load_data(path)

    def load_data(self, path):
        if not self.data:
            print('Pulling Acled Data')
            response = requests.get(ACLED_API)
            self.data = response.json()['data']
            dump_json_to_file(self.data_filename, self.data)
        print('Re-calculating Acled Data')
        self.summary = {
            'average': AcledApi.get_summary(self.data),
            'all': AcledApi.get_summary_with_type(self.data)
        }
        dump_json_to_file(self.summary_filename, self.summary)

    def get_data_pulled_dt(self):
        return get_file_created_iso_date(self.data_filename)

    @staticmethod
    def get_summary(data):
        summary = {}
        dataframe = pandas.DataFrame(data)
        dataframe = dataframe[dataframe.event_date >= '2008-01-01']
        op = dataframe.groupby(
            ['iso3', 'year']
        ).agg({'data_id': 'count'})
        summary = dataframe_to_nested_dict(op)
        return summary

    @staticmethod
    def get_summary_with_type(data):
        def get_event_type(index):
            return normal_to_camel_case(data[index].get('event_type'))

        def get_month(index):
            return data[index].get('event_date')[:7]

        dataframe = pandas.DataFrame(data)
        dataframe = dataframe[dataframe.event_date >= '2008-01-01']
        op = dataframe.groupby(
            ['iso3', get_event_type, get_month]
        ).agg({'data_id': 'count'})
        summary = dataframe_to_nested_dict(op)
        return summary

    def get_num_of_reported_conflict_events_pull(self, country_iso, year):
        data = self.summary['average'].get(
            get_country_iso3(country_iso), {}
        ).get(str(year), 0)
        return data

    def get_num_of_reported_conflict_events_average(self, country_iso):
        """
        # of reported conflict events (last 10 years average)
        """
        year_data = {}
        years = [year for year in range(2008, 2018 + 1)]
        for year in years:
            year_data[year] = self.get_num_of_reported_conflict_events_pull(
                country_iso, year,
            )
        avg = float(sum(year_data.values())) / max(len(year_data.keys()), 1)
        return ACLED_API, avg

    def get_num_of_reported_conflict_events_full(self, country_iso):
        """
        # of reported conflict events (last 10 years average)
        """
        return ACLED_API, self.summary['all'].get(get_country_iso3(country_iso), {})
