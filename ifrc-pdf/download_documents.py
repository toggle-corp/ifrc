import os
import json
import sys
import requests
import xmltodict
import traceback
import logging

from utils import async_download

logger = logging.getLogger(__name__)

LIMIT = 5

HEADERS = {'user-agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:63.0) Gecko/20100101 Firefox/63.0'}

TYPES = (
    ('epoa', 'http://www.ifrc.org/Utils/Search/Rss.ashx?at=241&c=&co=&dt=1&f=2018&feed=appeals&re=&t=2018&ti=&zo='), # noqa
    ('ou', 'http://www.ifrc.org/Utils/Search/Rss.ashx?at=56&c=&co=&dt=1&feed=appeals&re=&ti=&zo='), # noqa
    ('fr', 'http://www.ifrc.org/Utils/Search/Rss.ashx?at=57&c=&co=&dt=1&feed=appeals&re=&ti=&zo='), # noqa
    ('ea', 'http://www.ifrc.org/Utils/Search/Rss.ashx?at=246&c=&co=&dt=1&feed=appeals&re=&ti=&zo='), # noqa
)


def exception_handler(exception, response=None, url=None):
    logger.error('*' * 44)
    logger.error('Bad URL for ' + str(url))
    logger.error(traceback.format_exc())
    logger.error('*' * 44)


def get_documents(cache_dir):
    def get_documents_for(url, d_type):
        response = requests.get(url)
        items = xmltodict.parse(response.content)['rss']['channel']['item']
        link_with_filenames = []
        for item in items:
            title = item.get('title')
            link = item.get('link')
            filename = os.path.join(
                cache_dir, 'pdf/{}/{}.pdf'.format(d_type, title)
            )
            link_with_filenames.append([link, filename])
        return link_with_filenames

    url_with_filenames = []
    for d_type, url in TYPES:
        documents = get_documents_for(url, d_type)
        url_with_filenames.extend(documents)
    file_meta_path = os.path.join(cache_dir, 'file_meta.json')
    os.makedirs(os.path.dirname(file_meta_path), exist_ok=True)
    with open(file_meta_path, 'w') as fp:
        json.dump({
            '{}__{}'.format(
                filename.split('/')[-2], filename.split('/')[-1]
            ): link
            for link, filename in url_with_filenames
        }, fp)
    return async_download(
        url_with_filenames,
        headers=HEADERS,
        exception_handler=exception_handler
    )


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    get_documents(cache_dir='.cache')
