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
import yaml
from bs4 import BeautifulSoup

def get_title(soup):
    return soup.select('article-title')

def extract_info(filename):
    """ Extract all info from articles """
    with open(filename) as f:
        xmla = f.read()
        soup = BeautifulSoup(xmla, features="lxml")
        title = get_title(soup) # FIXME
        # print(title)
        for p in get_paragraphs(soup):
            # p['title'] = title
            # print(p)
            yield p

def scrub(paragraph):
    txt = ftfy.fixes.fix_line_breaks(paragraph['text'])
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
                'text': p.get_text(" ", strip=True).replace('\n', ' ')}


def write_to_dataset(dataset_filename): 
    ''' 
    Coroutine to write to a textacy dataset - format is one json per line -
    with given filename `dataset_filename`.
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

def load_metadata(yaml_file, metadata_path : str ='pdfs'):
    """
    Returns a dict where  
    - the key is the filename and 
    - the value is the information about the document
    """
    d = yaml.safe_load(yaml_file)
    return dict([(v['filename'], v) for v in d[metadata_path]] )

@click.command()
@click.argument('input_filepath', default='data/processed', type=click.Path(exists=True))
@click.argument('metadata_file', default='docs.yml', type=click.File('r'))
@click.argument('output_filepath', type=click.Path(), default='data/interim/blockchain_papers_dataset/dataset.json')
def main(input_filepath, metadata_file, output_filepath):
    """ Runs data processing scripts to turn raw data from (../raw) into
        cleaned data ready to be analyzed (saved in ../processed).
    """
    logger = logging.getLogger(__name__)
    logger.info('making final data set from raw data')
    logger.info(input_filepath)
    logger.info(output_filepath)

    metadata = load_metadata(metadata_file)

    sink = write_to_dataset(output_filepath)
    sink.__next__()

    for filename in os.listdir(input_filepath):
        name, extension = os.path.splitext(filename)
        if not extension == "cermxml": continue
        
        filepath = os.path.join(input_filepath, filename)
        if not os.path.isfile(filepath): continue

        logging.info("Processing " + filepath)
        meta = metadata.get(name + ".pdf", {})
        for paragraph in extract_info(filepath):
            paragraph.update(meta)
            sink.send(paragraph)
    sink.close()

    logger.info("finished")


if __name__ == '__main__':
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
