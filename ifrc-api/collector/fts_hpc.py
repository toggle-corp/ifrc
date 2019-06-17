"""
@author: eoglethorpe

pulling data from FTS for IFRC GO.

>> Number of CERF Operations and HRPs launched (by type)
>> count of emergencies by country by year with sum of total funding for
    funding reuqested vs funding recieved

1) pulls all values from country listing API

2) queries
    https://api.hpc.tools/v1/public/fts/flow?countryISO3=XXX&groupby=year
    for aggreagted yearly values for funding

3) queries https://api.hpc.tools/v1/public/emergency/country/{0}
    to get counts by emergency

***could have consolidated in one single call without grouping and then done
    the grouping manually,
    but the grouping is a little complex

run with:
        f = fts(test=True)
        r = f.merge()
        r
"""

import re
import json
from pandas.io.json import json_normalize
import dateparser
import traceback
from os.path import join as path_join

from .country import get_country_iso3, countries_iso3
from .common import sync_fetch, base64_encode, load_json_from_file, dump_json_to_file


API_END_POINT = 'https://api.hpc.tools/v1/public'
FTS_URL = API_END_POINT + '/fts/flow?countryISO3={0}&groupby=year&report=3'
EMERGENCY_URL = API_END_POINT + '/emergency/country/{0}'


def request_exception_handler(request, exception):
    print('*' * 44)
    print('Bad URL for ' + str(request.url))
    print(exception)


def api_pull(urls, headers):
    """pull down API contents, and use local history if testing
         urls: list of URLS to pull response from

        results yield the following:
            Sum of incoming flows grouped by the specified source object type
            Sum of incoming flows grouped by the specified destination object
                type
            ***Sum of incoming and internal flows grouped by the specified
                destination object type of minus the sum outgoing and internal
                flows grouped by source objects
            Sum of outgoing flows grouped by destination objects

        we want the ***'d one (report 3)
    TODO: Use asyncio
        https://pawelmhm.github.io/asyncio/python/aiohttp/2016/04/22/asyncio-aiohttp.html
    """
    if type(urls) != list:
        urls = [urls]

    # print('1st pulling for : ' + str(urls[0]))

    resps = sync_fetch(
        urls,
        headers=headers,
        exception_handler=request_exception_handler
    )
    """
    resps = []
    resps += grequests.map(
        rs, exception_handler=request_exception_handler, size=25
    )
    # rs = (grequests.get(ref) for ref in urls)
    """

    # print('reqs mapped')

    good_resps = []
    bad_resps = []
    for r in resps:
        load = r['json']
        load['url'] = str(r['url'])
        if r['status'] == 200:
            good_resps.append(load)
        else:
            bad_resps.append(load)

    # print('pulled. num bad resps: ' + str(len(bad_resps)))

    return good_resps


class FTS(object):
    DATA_FILENAME = 'funds.json'

    def __init__(self, hpc_credential, path, test=None, use_cache=False):
        """
        hpc_up
            - username:password
        """
        self.test = test
        self.funds = None
        self.headers = {
            'Authorization': 'Basic %s' % base64_encode(hpc_credential)
        }
        self.data_filename = path_join(path, FTS.DATA_FILENAME)
        if use_cache:
            try:
                self.funds = load_json_from_file(self.data_filename)
            except FileNotFoundError:
                print('Cache File Not Found... Calculating Data')
                self.merge()

    def get_urls(self, url):
        """
        iterate through cnts to get base URLs for sending to API in bulk

        """
        urls = [url.format(v) for v in countries_iso3]

        if self.test:
            return urls[:1]
        else:
            return urls

    def pull_funds(self):
        """
        go through URL list and pull needed info on total, pledged funding
        and total count
        """
        ret_d = {}

        urls = self.get_urls(FTS_URL)

        for cnt_vals in api_pull(urls, self.headers):

            # here we pull the ISO from the URL; we could have gotten this
            # at the api_pull, but #yolo (and it'd take some refactoring)
            iso = re.search('ISO3=([A-Z]{3})', cnt_vals['url']).group(1)
            ret_d[iso] = {}

            for fund_area in ['fundingTotals', 'pledgeTotals']:
                data = cnt_vals['data']['report3'][fund_area]['objects']
                if len(data) > 0:
                    for v in data[0]['objectsBreakdown']:
                        try:
                            year = int(v['name'])
                        except ValueError:
                            traceback.print_exc()
                            continue
                        if year not in ret_d[iso]:
                            ret_d[iso][year] = {fund_area: v['totalFunding']}

                    ret_d[iso][year][fund_area] = v['totalFunding']

        return ret_d

    def pull_evt_cnts(self):
        """
        go through URL list and pull needed info on counts by country and year
        """
        urls = self.get_urls(EMERGENCY_URL)
        ret_d = {}

        for v in api_pull(urls, self.headers):
            iso = iso = re.search('([A-Z]{3})$', v['url']).group(1)
            if iso in ret_d:
                print('Duplicate iso {}'.format(iso))
            ret_d[iso] = {}

            # extract years and group by them
            r = json_normalize(v['data']).apply(
                lambda x: dateparser.parse(x.date).year, axis=1
            )
            try:
                s = r.groupby(r).size()
            except ValueError:  # Raised if v['data'] is empty array
                pass

            for v in s.iteritems():
                # item 0: year, 1: count
                ret_d[iso][v[0]] = v[1]

        return ret_d

    def merge(self):
        """
        join counts and funding amts by building on funds dict
        """
        print('>> Pulling FTS Data')
        funds = self.pull_funds()
        cnts = self.pull_evt_cnts()

        # k: country, v: values
        for country, values in cnts.items():
            if country not in funds:
                funds[country] = {}

            # iterate through the dict containing ik:year iv:counts
            for year, counts in values.items():
                if year not in funds[country]:
                    funds[country][year] = {}

                funds[country][year]['numActivations'] = counts

        self.funds = funds
        dump_json_to_file(self.data_filename, self.funds)
        return self.funds

    def get_data(self, country_iso2):
        return self.funds.get(get_country_iso3(country_iso2))


if __name__ == '__main__':
    f = FTS(test=True)
    print(json.dumps(f.merge()))
