import os
import click
import time
import json
from pandas.io.json import json_normalize
import traceback
from extractor import MetaFieldExtractor, SectorFieldExtractor
from config import (
    _mfd,
    _s,
    _sfd,
)
from download_documents import get_documents as download_documents
from common import json_preety, seconds_to_human_readable
from utils import (
    convert_pdf_to_text_blocks, get_files_in_directory
)

sectors = [
    _s.health, _s.shelter, _s.livelihoods_and_basic_needs,
    _s.Water_sanitation_hygiene, _s.disaster_Risk_reduction,
    _s.protection_gender_inclusion, _s.migration,
]
sector_fields = [
    _sfd.male, _sfd.female, _sfd.requirements, _sfd.people_targeted,
]

epoa_fields = [
    _mfd.appeal_number, _mfd.glide_number,
    _mfd.date_of_issue, _mfd.appeal_launch_date,
    _mfd.expected_time_frame, _mfd.expected_end_date,
    _mfd.category_allocated, _mfd.dref_allocated,
    _mfd.num_of_people_affected,
    _mfd.num_of_people_to_be_assisted,
]
ou_fields = [
    _mfd.glide_number,
    _mfd.appeal_number,
    _mfd.date_of_issue,
    _mfd.epoa_update_num,
    _mfd.time_frame_covered_by_update,
    _mfd.operation_start_date,
    _mfd.operation_timeframe,
]
fr_fields = [
    # DREF/Emergency Appeal/One internal Appeal Number:
    _mfd.appeal_number,  # Operation number: MDRxx…
    _mfd.date_of_issue,
    _mfd.glide_number,
    _mfd.date_of_disaster,
    _mfd.operation_start_date,
    _mfd.operation_end_date,
    _mfd.overall_operation_budget,
    _mfd.num_of_people_affected,
    _mfd.num_of_people_to_be_assisted,
    _mfd.num_of_partner_ns_involved,
    _mfd.num_of_other_partner_involved,
]
ea_fields = [
    _mfd.appeal_number,
    _mfd.glide_number,
    _mfd.num_of_people_to_be_assisted,
    _mfd.dref_allocated,
    _mfd.current_operation_budget,
    # _mfd.funding_gap,
    _mfd.appeal_launch_date,
    # _mfd.Revision number,
    _mfd.appeal_ends,
    # _mfd.Extended X months,
]

pdf_types = (
    ('epoa', epoa_fields),
    ('ou', ou_fields),
    ('fr', fr_fields),
    ('ea', ea_fields),
)


def print_meta_stats(data, fields):
    missed = []
    empty = []
    for field in fields:
        if field not in data:
            missed.append(field)
        elif not data.get(field):
            empty.append(field)
    if len(missed):
        print('MISSING:', missed)
    if len(empty):
        print('EMPTY:', empty)
    print(json_preety(data))


def print_sector_stats(data, sectors, fields):
    missed = []
    empty = []
    for sector in sectors:
        for field in fields:
            s_f = '{}__{}'.format(sector, field)
            if s_f not in data:
                missed.append((sector, field))
            elif not data.get(s_f):
                empty.append((sector, field))
    if len(missed):
        print('MISSING:', missed)
    if len(empty):
        print('EMPTY:', empty)
    print(json_preety(data))


@click.command()
@click.option(
    '--cache-dir', prompt='Cache directory', help='Cache directory'
)
@click.option(
    '--output-dir', prompt='Output directory', help='Output directory'
)
@click.option(
    '--download', default='True', help='Download files. Default True'
)
@click.option(
    '--test', default='False', help='Test, use few files'
)
def start_extraction(cache_dir, output_dir, download, test):
    FILES_DIR = os.path.join(cache_dir, 'pdf')
    OUTPUT_WITH_SCORE = {}
    OUTPUT = {}
    ERROR_FILES = []
    start = time.time()

    if download.lower() == 'true':
        download_documents(cache_dir=cache_dir)

    with open(os.path.join(cache_dir, 'file_meta.json')) as fp:
        file_meta = json.load(fp)

    for directory, fields in pdf_types:
        files = get_files_in_directory(os.path.join(FILES_DIR, directory))
        if test.lower() == 'true':
            files = files[:5]
        for file in files:
            try:
                filename = file.split('/')[-1]
                print('{0}-{1}-{0}'.format('*' * 10, file))
                texts = convert_pdf_to_text_blocks(file, cache_dir=cache_dir)
                m_texts = texts[:texts.index('Page 2')]
                m_extractor = MetaFieldExtractor(m_texts, fields)
                s_extractor = SectorFieldExtractor(texts, sectors, sector_fields)
                m_data_with_score, m_data = m_extractor.extract_fields()
                s_data_with_score, s_data = s_extractor.extract_fields()
                # print_meta_stats(m_data, fields)
                # print_sector_stats(s_data, sectors, sector_fields)
                if not OUTPUT.get(directory):
                    OUTPUT[directory] = []
                if not OUTPUT_WITH_SCORE.get(directory):
                    OUTPUT_WITH_SCORE[directory] = []
                OUTPUT[directory].append({
                    'filename': file.split('/')[-1],
                    'url': file_meta.get('{}__{}'.format(directory, filename)),
                    'meta': m_data,
                    'sector': s_data,
                })
                OUTPUT_WITH_SCORE[directory].append({
                    'filename': file.split('/')[-1],
                    'url': file_meta.get('{}__{}'.format(directory, filename)),
                    'meta': m_data_with_score,
                    'sector': s_data_with_score,
                })
            except Exception as e:
                print(file, traceback.format_exc())
                OUTPUT[directory].append({
                    'filename': file.split('/')[-1],
                    'url': file_meta.get('{}__{}'.format(directory, filename)),
                })
                OUTPUT_WITH_SCORE[directory].append({
                    'filename': file.split('/')[-1],
                    'url': file_meta.get('{}__{}'.format(directory, filename)),
                })
                ERROR_FILES.append(file)

    print('Total Time:', seconds_to_human_readable(time.time() - start))
    print('*' * 11, 'ERROR FILES', '*' * 11)
    print(len(ERROR_FILES), ERROR_FILES)

    print('\n **** Saving ****\n')
    output_js = os.path.join(output_dir, 'output.json')
    output_js_with_score = os.path.join(output_dir, 'output_with_score.json')
    os.makedirs(os.path.dirname(output_js), exist_ok=True)
    os.makedirs(os.path.dirname(output_js_with_score), exist_ok=True)
    with open(output_js, 'w') as fp:
        json.dump(OUTPUT, fp)
        print('Saved to {}'.format(output_js))
    with open(output_js_with_score, 'w') as fp:
        json.dump(OUTPUT_WITH_SCORE, fp)
        print('Saved to {}'.format(output_js_with_score))

    for pdf_type, fields in pdf_types:
        output_filename = os.path.join(output_dir, 'csv/{}.csv').format(pdf_type)
        output_with_score_filename = os.path.join(
            output_dir,
            'csv/{}_with_score.csv'
        ).format(pdf_type)
        os.makedirs(os.path.dirname(output_filename), exist_ok=True)
        os.makedirs(os.path.dirname(output_with_score_filename), exist_ok=True)
        output = json_normalize(OUTPUT.get(pdf_type))
        output.to_csv(output_filename)
        print('Saved to {}'.format(output_filename))

        output_with_score = json_normalize(OUTPUT_WITH_SCORE.get(pdf_type))
        output_with_score.to_csv(output_with_score_filename)
        print('Saved to {}'.format(output_with_score_filename))


if __name__ == '__main__':
    start_extraction()
# start_extraction(cache_dir='.cache', output_dir='output')
