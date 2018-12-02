import re
import os
import pickle
import base64
import requests
import csv
import json
import aiohttp
import asyncio
from datetime import datetime, timezone, timedelta, date as datetime_date
from dateutil.relativedelta import relativedelta

from .config import settings


datetime_now = datetime.now()


def normal_to_camel_case(name=''):
    return ''.join(x for x in name.title() if not x.isspace())


def snakecase_to_camelCase(name):
    components = name.split('_')
    # We capitalize the first letter of each component except the first one
    # with the 'title' method and join them together.
    return components[0] + ''.join(x.title() for x in components[1:])


def camel_case_to_snake_case(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def gen_output_path(path, output_dir=None):
    return os.path.join(
        output_dir if output_dir else settings.output_dir, path
    )


def get_file_created_iso_date(filename):
    return datetime.fromtimestamp(os.path.getmtime(filename)).isoformat()


def to_iso_date(year, month=1, day=1):
    return datetime(
        year=year, month=month, day=day, tzinfo=timezone.utc,
    ).isoformat()


def get_iso_month_start_end_day(year, month, isoformat=False):
    date = datetime(year=year, month=month, day=1, tzinfo=timezone.utc)
    last_day = date + relativedelta(day=1, months=+1, days=-1)
    first_day = date + relativedelta(day=1)
    if not isoformat:
        return first_day, last_day
    return first_day.isoformat(), last_day.isoformat()


def now_iso_date():
    return to_iso_date(
        datetime_now.year, datetime_now.month, datetime_now.day
    )


def get_year_range_by_offset(years):
    return (datetime_now - relativedelta(years=years)).year, datetime_now.year


def get_months_between_years(start_year, end_year):
    months = []
    for year in range(start_year, end_year + 1):
        for month in range(1, 12 + 1):
            months.append({'year': year, 'month': month})
    return months


def get_months_from_years(years):
    return get_months_between_years(*get_year_range_by_offset(years=years))


def get_year_month_formatted(date=None, year=None, month=None):
    _date = date if date else datetime_date(year=year, month=month, day=1)
    return _date.strftime('%Y-%m')


def load_json_from_file(filename):
    with open(filename, 'r') as fp:
        data = json.load(fp)
    return data


def load_pickle_from_file(filename):
    with open(filename, 'rb') as fp:
        data = pickle.load(fp)
    return data


def dump_url_to_file(url, filename):
    response = requests.get(url, allow_redirects=True)
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, 'wb') as fp:
        fp.write(response.content)


def dump_json_to_file(filename, data):
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, 'w') as fp:
        json.dump(data, fp)


def dump_pickle_to_file(filename, data):
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, 'wb') as fp:
        pickle.dump(data, fp)


def load_csv_to_dict(path, newline=''):
    data = []
    with open(
            path, newline=newline, errors='ignore', encoding='utf-8'
    ) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            data.append(row)
    return data


def base64_encode(string):
    return base64.urlsafe_b64encode(
        string.encode('UTF-8')
    ).decode('ascii')


def seconds_to_human_readable(seconds):
    sec = timedelta(seconds=seconds)
    return str(sec)


async def r_fetch(session, url, headers, return_param=None, exception_handler=None):
    async with session.get(url, headers=headers) as response:
        try:
            return {
                'json': await response.json(),
                'url': str(response.url),
                'status': response.status,
            }, return_param
        except Exception as e:
            if exception_handler is not None:
                exception_handler(response, e)


async def r_post(session, url, param, return_param=None, exception_handler=None):
    async with session.post(url, data=json.dumps(param)) as response:
        try:
            return {
                'json': await response.json(),
                'url': str(response.url),
                'status': response.status,
            }, return_param
        except Exception as e:
            if exception_handler is not None:
                exception_handler(response, e)


async def _async_fetch(url_with_params, headers, exception_handler):
    tasks = []
    async with aiohttp.ClientSession() as session:
        for url, return_params in url_with_params:
            task = asyncio.ensure_future(
                r_fetch(session, url, headers, return_params, exception_handler)
            )
            tasks.append(task)
        responses = await asyncio.gather(*tasks)
        return responses


async def _async_post(urls_with_params, exception_handler=None):
    tasks = []
    async with aiohttp.ClientSession() as session:
        for url, param, return_params in urls_with_params:
            task = asyncio.ensure_future(
                r_post(session, url, param, return_params, exception_handler)
            )
            tasks.append(task)
        responses = await asyncio.gather(*tasks)
        return responses


def async_fetch(url_with_params, headers=None, exception_handler=None):
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(
        _async_fetch(url_with_params, headers, exception_handler)
    )
    return loop.run_until_complete(future)


def async_post(urls_with_params, exception_handler=None):
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(
        _async_post(urls_with_params, exception_handler)
    )
    return loop.run_until_complete(future)


def sync_fetch(urls, headers=None, exception_handler=None):
    for url in urls:
        try:
            response = requests.get(url, headers=headers)
            yield {
                'json': response.json(),
                'url': str(response.url),
                'status': response.status_code,
            }
        except Exception as e:
            if exception_handler is not None:
                exception_handler(response, e)
