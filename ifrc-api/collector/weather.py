import cdsapi
import pygrib

from .country import countries
from .common import sync_fetch


"""
https://datahelpdesk.worldbank.org/knowledgebase/articles/902061-climate-data-api
"""
WB_ENDPOINT = 'http://climatedataapi.worldbank.org/climateweb/rest/v1'
WB_API = WB_ENDPOINT + '/country/{type}/{var}/{start}/{end}/{ISO3}'


def run_cdsapi():
    c = cdsapi.Client(
        url='https://cds.climate.copernicus.eu/api/v2',
        key='2496:2790f5a7-b4b7-47d5-9a3f-eeeb7fd47d3b',
        verify=0,
    )

    r = c.retrieve(
        'reanalysis-era5-single-levels',
        {
            'variable': 'total_precipitation',
            'product_type': 'reanalysis',
            'year': '2000',
            'month': '01',
            'day': '01',
            'time': [
                '07:00', '08:00', '09:00',
                '10:00', '11:00', '12:00',
                '13:00', '14:00', '15:00',
                '16:00', '17:00', '18:00',
                '19:00', '20:00', '21:00',
                '22:00', '23:00'
            ],
            'format': 'grib'
        })

    r.download('.cache/download.grib')
    grbs = pygrib.open('.cache/download.grib')
    for grb in grbs:
        print(grb.values)


def request_exception_handler(request, exception):
    print('{} Failed'.format(request.url))


def run_wb():
    urls = [
        WB_API.format(
            type='mavg',
            var='pr',
            start='2008',
            end='2018',
            ISO3=country['iso3'],
        ) for country in countries
    ]
    resps = sync_fetch(
        urls,
        headers={},
        exception_handler=request_exception_handler
    )
    for r in resps:
        print(r['url'], r['json'])
