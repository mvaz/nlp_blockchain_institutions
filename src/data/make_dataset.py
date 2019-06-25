# -*- coding: utf-8 -*-
import click
import logging
from pathlib import Path
from dotenv import find_dotenv, load_dotenv

from io import StringIO
import os
import re
import textacy
import ftfy
import json
from bs4 import BeautifulSoup

def get_title(soup):
    return soup.select('article-title')

def extract_info(filename):
    """ Extract all info from articles """
    with open(filename) as f:
        xmla = f.read()
        soup = BeautifulSoup(xmla, features="lxml")
        title = get_title(soup)
        # print(title)
        for p in get_paragraphs(soup):
            # p['title'] = title
            # print(p)
            yield p

def scrub(paragraph):
    txt = ftfy.fixes.fix_line_breaks(paragraph['raw_text'])
    txt = textacy.preprocess.normalize_whitespace(txt)
    return txt

def get_paragraphs(soup):
    """ Extract text from paragraphs """
    for sec in soup.find_all('sec'):
        # print(sec['id'])
        for i, p in enumerate(sec.find_all('p')):
            # logging.info
            # logging.info(p)
            yield {
                'section_id': sec['id'],
                'paragraph_id': i,
                'raw_text': p.get_text(" ", strip=True).replace('\n', ' ')}


def write_to_dataset(dataset_filename): 
    ''' 
    Write to textacy dataset with given filename 
    '''
    logging.info("Opening dataset file") 
    with open(dataset_filename, 'w+') as f:
        try: 
            while True: 
                paragraph = (yield) 
                pt = json.dumps(paragraph)
                f.write(pt)
                f.write('\n')
        except GeneratorExit: 
            logging.info("Finalized dataset!") 

@click.command()
@click.argument('input_filepath', default='data/processed', type=click.Path(exists=True))
@click.argument('output_filepath', type=click.Path(), default='data/interim/dataset.json')
def main(input_filepath, output_filepath):
    """ Runs data processing scripts to turn raw data from (../raw) into
        cleaned data ready to be analyzed (saved in ../processed).
    """
    logger = logging.getLogger(__name__)
    logger.info('making final data set from raw data')
    logger.info(input_filepath)
    logger.info(output_filepath)

    sink = write_to_dataset(output_filepath)
    sink.__next__()

    for x in os.listdir(input_filepath):
        x_ = os.path.join(input_filepath, x)
        logging.info("Processing " + x_)
        if not os.path.isfile(x_) or not x_.endswith('xml'): continue
        for paragraph in extract_info(x_):
            paragraph['paper'] = x
            sink.send(paragraph)
    sink.close()



if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
