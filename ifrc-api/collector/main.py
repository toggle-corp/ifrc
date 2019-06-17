import time

from .country import countries
from .config import Strings, Fields
from .common import (
    gen_output_path, now_iso_date, seconds_to_human_readable, dump_json_to_file
)

from .acled_api import AcledApi
from .reliefweb_api import ReliefWebApi
from .go_api import (
    GoApi, Regions_Id, RegionName
)
from .startnetwork import StartNetworkApi
from .world_population import get_world_population
from .fts_hpc import FTS


NUMBER_OF_COUNTRIES = len(countries)
NUMBER_OF_REGIONS = len(Regions_Id)


def print_break(text=None, char='-', len=22):
    if text:
        print('\n{}-{}-{}\n'.format(char*int(len/2), text, char*int(len/2)))
    else:
        print('\n', '-' * len, '\n')


def print_region_status(region_id, index):
    print(
        'Collecting Data for Region: {} -- {} out of {}'.
        format(RegionName[region_id], index + 1, NUMBER_OF_REGIONS)
    )


def print_countries_status(country, index):
    print(
        'Collecting Data for Country: {} -- {} out of {}'.
        format(country['iso'], index + 1, NUMBER_OF_COUNTRIES)
    )


def print_pull_info(source, info):
    print('\t >> {}->{}'.format(source, info))


class GoDataSourceCollector():

    def __init__(
            self,
            path,
            hpc_credential,
            test=False,
            use_cache=False,
    ):
        """
        hpc_up
            - username:password
        """
        print_break('Initializing')
        self.test = test
        self.use_cache = use_cache
        self.acledApi = AcledApi(gen_output_path('acleddata', path), use_cache=use_cache)
        self.startnetwork = StartNetworkApi(
            gen_output_path('startnetwork', path), use_cache=use_cache
        )
        self.goApi = GoApi(gen_output_path('go_api', path), use_cache=use_cache)
        self.population = get_world_population(use_cache)
        """
        import pytz
        import datetime
        DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S %Z"
        TIMEZONE = pytz.timezone('Asia/Kathmandu')
        start_datetime = datetime.datetime.now(TIMEZONE)
        """
        self.fts = FTS(
            hpc_credential, gen_output_path('fts', path), test=test, use_cache=use_cache,
        )
        """
        print(
            '%s -- %s' % (
                start_datetime.strftime(DATETIME_FORMAT),
                datetime.datetime.now(TIMEZONE).strftime(DATETIME_FORMAT)
            )
        )
        """

    def get_country_data(self, country):
        """
            country_id is only for go_api
        """
        iso = country['iso']
        country_id = country['id']

        country_data = {
            'country': iso,
        }

        print_pull_info('ReliefWebApi', 'get_count_of_reported_events_10y')
        # Relief # of reported events (last 10 years average)
        url, value = ReliefWebApi.get_count_of_reported_events_10y(iso)
        country_data['numReportedEvents'] = [
            {
                Fields.value: value,
                Fields.source_url: url,
                Fields.source: Strings.sources.reliefWeb,
                Fields.date_pulled: now_iso_date(),
                Fields.unit: Strings.units.count,
            },
        ]

        print_pull_info('AcledApi', 'get_num_of_reported_conflict_events')
        # Acled # of reported conflict events (last 10 years average)
        url, value = self.acledApi.get_num_of_reported_conflict_events_average(iso)
        country_data['numReportedEvents'].append({
            Fields.value: value,
            Fields.source_url: url,
            Fields.source: Strings.sources.acled,
            Fields.date_pulled: self.acledApi.get_data_pulled_dt(),
            Fields.unit: Strings.units.average,
        })

        print_pull_info('AcledApi', 'get_num_of_reported_conflict_events_full')
        # Acled # of reported conflict events (last 10 years average)
        url, value = self.acledApi.get_num_of_reported_conflict_events_full(iso)
        country_data['numReportedEvents'].append({
            Fields.value: value,
            Fields.source_url: url,
            Fields.source: Strings.sources.acled,
            Fields.date_pulled: self.acledApi.get_data_pulled_dt(),
        })

        print_pull_info('ReliefWebApi', 'get_count_of_reported_events_filtered_10y')
        url, value = ReliefWebApi.get_count_of_reported_events_filtered_10y(
            iso
        )
        country_data['numOfOperationsByEpidemicType'] = {
            Fields.value: value,
            Fields.source_url: url,
            Fields.source: Strings.sources.reliefWeb,
            Fields.date_pulled: now_iso_date(),
            Fields.unit: Strings.units.count,
        }

        print_pull_info('ReliefWebApi', 'get_latest_disaster')
        country_data['latestDisaster'] = ReliefWebApi.get_latest_disaster(iso)

        print_pull_info('Startnetwork', 'get_num_of_operations_by_crisis_type')
        url, value = self.startnetwork.get_num_of_operations_by_crisis_type(iso)
        country_data['numOfOperationsByCrisisType'] = {
            Fields.value: value,
            Fields.source_url: url,
            Fields.source: Strings.sources.acled,
            Fields.date_pulled: self.startnetwork.get_data_pulled_dt(),
            Fields.unit: Strings.units.count,
        }

        print_pull_info(
            'GoApi', 'latest_operation_appeal_DREF_with_budget_and_targeted_beneficiaries'
        )
        url, value = GoApi.\
            latest_operation_appeal_DREF_with_budget_and_targeted_beneficiaries(
                country_id,
            )
        country_data['latestAppeal'] = {
            Fields.value: value,
            Fields.source_url: url,
            Fields.source: Strings.sources.go_api,
            Fields.date_pulled: now_iso_date(),
        }

        print_pull_info('GoApi', 'num_of_op_that_IFRC_launched_to_by_type')
        country_data['appeals'] = self.goApi.\
            num_of_op_that_IFRC_launched_to_by_type(country_id)

        print_pull_info('FTS', 'fts.get_data')
        country_data['fts'] = self.fts.get_data(iso)

        print_pull_info('Population', 'population.get')
        country_data['population'] = self.population.get(iso, [])

        return country_data

    def collect(self):
        self.country_collector = {}
        self.region_collector = {}
        print_break('Collecting Data')

        for index, region_id in enumerate(Regions_Id):
            print_region_status(region_id, index)
            self.region_collector[RegionName[region_id]] = {
                'appeals': self.goApi.num_of_op_that_IFRC_launched_to_by_type(
                        region_id=region_id
                    ),
            }

        print_break(len=44)

        for index, country in enumerate(countries):
            print_countries_status(country, index)
            self.country_collector[country['iso']] = self.get_country_data(country)
            if self.test:
                break

    def dump_json(self, path):
        if self.region_collector is None or self.country_collector is None:
            raise Exception('First call collect()')
        print('-' * 44)
        print('Saving to json: {}'.format(path))
        collector = {
            'regions': self.region_collector,
            'countries': self.country_collector,
        }
        dump_json_to_file(path, collector)

    def dump_csv(self):
        """
        print('Saving to csv')
        headers = ['id', 'country']
        ToCsv(
            headers=headers, key_separator='__',
        ).save_csv(gen_output_path('test.csv'), collector)
        """
        pass


if __name__ == '__main__':
    start = time.time()
    collector = GoDataSourceCollector('.cache')
    collector.collect()
    collector.dump_json('output/output.json')
    print(seconds_to_human_readable(time.time() - start))
