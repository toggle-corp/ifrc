"""
from io import StringIO
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfpage import PDFPage

# converts pdf, returns its text content as a string
def convert_pdf_to_text(fname, pages=None):
    if not pages:
        pagenums = set()
    else:
        pagenums = set(pages)

    with StringIO() as output:
        manager = PDFResourceManager()
        converter = TextConverter(manager, output, laparams=LAParams())
        interpreter = PDFPageInterpreter(manager, converter)

        with open(fname, 'rb') as infile:
            for page in PDFPage.get_pages(infile, pagenums):
                print('herer')
                interpreter.process_page(page)

        converter.close()
        text = output.getvalue()
    return text
"""
import os
import re
import PyPDF2
import asyncio
import aiohttp
import aiofiles
import logging
from io import BytesIO
from bs4 import BeautifulSoup as bsoup

from pdfminer.converter import HTMLConverter
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.layout import LAParams
from pdfminer.pdfpage import PDFPage

from tidylib import tidy_document

logger = logging.getLogger(__name__)


def re_ignorecase_text(text):
    return re.compile(text, re.IGNORECASE)


def get_files_in_directory(directory):
    try:
        return [
            os.path.join(directory, file) for file in os.listdir(directory)
            if os.path.isfile(os.path.join(directory, file))
        ]
    except FileNotFoundError:
        return []


def convert_pdf_to_text(file):
    readreport = PyPDF2.PdfFileReader(open(file, 'rb'))
    text = []
    for i in range(0, readreport.numPages):
        pageobj = readreport.getPage(i)
        text.append(pageobj.extractText())
    return ' '.join(text)


def convert_pdf_to_html(file, cache_dir):
    html_file = os.path.join(cache_dir, 'html/{}/{}.html'.format(
        os.path.basename(os.path.dirname(file)), os.path.basename(file)
    ))
    text_file = os.path.join(cache_dir, 'text/{}/{}.text'.format(
        os.path.basename(os.path.dirname(file)), os.path.basename(file)
    ))
    if os.path.isfile(html_file):
        with open(html_file, 'r') as fp:
            return fp.read()
    rsrcmgr = PDFResourceManager()
    with BytesIO() as retstr:
        laparams = LAParams()
        with HTMLConverter(
                rsrcmgr, retstr, codec='utf-8', laparams=laparams,
        ) as device:
            with open(file, 'rb') as fp:
                interpreter = PDFPageInterpreter(rsrcmgr, device)
                maxpages = 0
                caching = True
                pagenos = set()
                for page in PDFPage.get_pages(
                        fp, pagenos, maxpages=maxpages,
                        caching=caching, check_extractable=True,
                ):
                    interpreter.process_page(page)

                text = retstr.getvalue().decode()
    html, errors = tidy_document(text)
    html = re.sub(r'\s\s+', ' ', html)
    text = convert_pdf_to_text(file)
    os.makedirs(os.path.dirname(html_file), exist_ok=True)
    os.makedirs(os.path.dirname(text_file), exist_ok=True)
    with open(text_file, 'w') as fp:
        fp.write(text)
    with open(html_file, 'w') as fp:
        fp.write(html)
    return html


def convert_pdf_to_text_blocks(file, cache_dir):
    html = convert_pdf_to_html(file, cache_dir=cache_dir).replace('\n', '')
    soup = bsoup(html, 'html.parser')
    texts = []
    for div in soup.find_all('div'):
        text = []
        for span in div.find_all(['span', 'a']):
            text.append(' '.join(span.get_text().split()))
        texts.append(' '.join(text).strip())
    # return [' '.join(texts)]
    return texts


def async_download(url_with_filenames, headers=None, exception_handler=None):
    async def _async_fetch():
        async def r_fetch(session, url, filename):
            logger.info('Downloading {} -> {}'.format(url, filename))
            try:
                async with session.get(url) as response:
                    try:
                        os.makedirs(os.path.dirname(filename), exist_ok=True)
                        async with aiofiles.open(filename, 'wb') as fp:
                            while True:
                                chunk = await response.content.read(1024)
                                if not chunk:
                                    break
                                await fp.write(chunk)
                        logger.info(
                            'Download Success {} -> {}'.format(url, filename)
                        )
                        return filename
                    except Exception as e:
                        if exception_handler is not None:
                            exception_handler(e, response=response, url=url, filename=filename)
            except Exception as e:
                if exception_handler is not None:
                    exception_handler(e, url=url, filename=filename)
        conn = aiohttp.TCPConnector(limit_per_host=5)
        tasks = []
        async with aiohttp.ClientSession(headers=headers, connector=conn) as session:
            for url, filename in url_with_filenames:
                task = asyncio.ensure_future(r_fetch(session, url, filename))
                tasks.append(task)
            responses = await asyncio.gather(*tasks)
            return responses

    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(_async_fetch())
    return loop.run_until_complete(future)
