'''
http://api.worldbank.org/countries/IRQ/indicators/SP.POP.TOTL?format=json&per_page=10&date=2008:2018
'''
from .country import countries_iso3, get_country_iso2
from .common import async_fetch

API_URL = 'http://api.worldbank.org/countries/{}/indicators/'\
          'SP.POP.TOTL?format=json&per_page=10&date=2008:2018'


def request_exception_handler(request, exception):
    print('*' * 44)
    print('Bad URL for ' + str(request.url))
    print(exception)


def get_world_population(no_cache=False):
    if no_cache:
        urls = [(API_URL.format(iso3), iso3) for iso3 in countries_iso3[:5]]
    else:
        urls = [(API_URL.format(iso3), iso3) for iso3 in countries_iso3]
    resps = async_fetch(
        urls,
        exception_handler=request_exception_handler
    )
    population = {}
    for response, iso3 in resps:
        try:
            population[get_country_iso2(iso3)] = [
                {'date': datum['date'], 'value': datum['value']}
                for datum in response['json'][1]
            ]
        except (IndexError, TypeError):
            print('Population failed for {}: {}'.format(iso3, response.get('json')))
    return population
