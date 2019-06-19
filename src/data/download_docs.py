#!/usr/bin/python3
#encoding:utf-8
#Tutorial: http://www.knight-of-pi.org/python-3-settings-file-with-yaml-for-data-serialization
#Licence: http://creativecommons.org/licenses/by-nc-sa/3.0/
# Author: Johannes Bergs

import click
import logging
from pathlib import Path
from dotenv import find_dotenv, load_dotenv

import yaml
from pprint import pprint
import os
import wget

url = 'http://i3.ytimg.com/vi/J---aiyznGQ/mqdefault.jpg'  


def download_url(url, file_output):
    logger = logging.getLogger(__name__)
    logger.info('downloading single document')
    pprint(url)
    pprint(file_output)
    wget.download(url, file_output)  

def handle_file():
    pass

@click.command()
@click.argument('docs', type=click.File('r'))
@click.argument('output_filepath', type=click.Path(exists=True))
def main(docs, output_filepath):
    """ Runs data processing scripts to turn raw data from (../raw) into
        cleaned data ready to be analyzed (saved in ../processed).
    """
    logger = logging.getLogger(__name__)
    logger.info('downloading documents to external data')
    logger.info(docs)
    logger.info(output_filepath)

    config = yaml.safe_load(docs)
    pprint(config['pdfs'])
    for doc in config['pdfs']:
        download_url(doc['url'], os.path.join(output_filepath, doc['filename']))



if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
