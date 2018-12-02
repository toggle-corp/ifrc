import time
# import datetime
import os

import click
from dotenv import load_dotenv

# Build paths inside the project like this: os.path.join(BASE_DIR, ...)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, '.env'))


@click.command()
@click.option(
    '--output-file', prompt='Output file', help='Output file'
)
@click.option(
    '--test', default='False', help='Test Run. Few Countries are collected'
)
def run(output_file, test):
    from collector import GoDataSourceCollector
    from collector.common import seconds_to_human_readable

    start = time.time()

    hpc_credential = os.environ['HPC_CREDENTIAL']

    collector = GoDataSourceCollector(
        path='.cache',
        hpc_credential=hpc_credential,
        test=test.lower() == 'true',
    )
    collector.collect()
    collector.dump_json(output_file)

    print(seconds_to_human_readable(time.time() - start))


if __name__ == '__main__':
    run()
