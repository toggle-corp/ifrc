from os.path import join as path_join
import json
import logging
import pandas

from .country import get_country_iso2
from .config import NotCacheException
from .utils import dataframe_to_nested_dict
from .common import (
    load_json_from_file, dump_json_to_file, load_csv_to_dict,
    dump_url_to_file, get_file_created_iso_date,
)

logger = logging.getLogger(__name__)

ALERTS_URL = 'https://startnetwork.org/api/v1/start-fund-all-alerts'


class StartNetworkApi():
    DATA_FILENAME = 'data.csv'
    SUMMARY_FILENAME = 'summary.json'

    def __init__(self, path, use_cache=False):
        self.data = None
        self.summary = None

        self.data_filename = path_join(path, StartNetworkApi.DATA_FILENAME)
        self.summary_filename = path_join(
            path, StartNetworkApi.SUMMARY_FILENAME
        )

        try:
            if not use_cache:
                raise NotCacheException()
            self.data = load_csv_to_dict(self.data_filename)
            self.summary = load_json_from_file(self.summary_filename)
            print('Using Local startnetwork Data')
        except (
                TypeError, FileNotFoundError, json.decoder.JSONDecodeError,
                NotCacheException,
        ):
            self.load_data(path)

    def get_data_pulled_dt(self):
        return get_file_created_iso_date(self.data_filename)

    def load_data(self, path):
        if not self.data:
            print('Pulling startnetwork Data')
            dump_url_to_file(ALERTS_URL, self.data_filename)
            self.data = load_csv_to_dict(self.data_filename)
        print('Re-calculating startnetwork Data')
        self.summary = self.get_summary(self.data)
        dump_json_to_file(self.summary_filename, self.summary)

    def get_crisis_types(self):
        crisis_types = {}
        for datum in self.data:
            crisis_type = datum.get('Crisis Type')
            if crisis_types.get(crisis_type):
                crisis_types[crisis_type] += 1
            else:
                crisis_types[crisis_type] = 1
        return crisis_types

    @staticmethod
    def get_summary(data):
        def get_iso2(index):
            country = data[index].get('Country').split(' [')[0]
            return get_country_iso2(name=country)

        def get_crisis_type(index):
            return data[index].get('Crisis Type').lower()

        dataframe = pandas.DataFrame(data)
        op = dataframe.groupby(
            [get_iso2, get_crisis_type]
        ).agg({'Country': 'count'})
        summary = dataframe_to_nested_dict(op)
        return summary

    def get_num_of_operations_by_crisis_type(self, country_iso=None):
        """
        # of Operations that Start Fund Responded to (by type)
        """
        return ALERTS_URL, self.summary.get(country_iso)
