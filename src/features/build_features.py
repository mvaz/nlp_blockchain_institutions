from bs4 import BeautifulSoup
import click
import logging
from pathlib import Path
from dotenv import find_dotenv, load_dotenv

import spacy
from spacy.lang.en import English

# Load English tokenizer, tagger, parser, NER and word vectors
nlp = English()

# from spacy.matcher import PhraseMatcher
# from spacy.tokens import Span


def get_title(soup):
    return soup.select('article-title')


def extract_info(filename):
    """ Extract all info from articles """
    with open(filename) as f:
        xmla = f.read()
        soup = BeautifulSoup(xmla, features="lxml")
        title = get_title(soup)
        paragraphs = get_paragraphs(soup)
    return {
        'title': title,
        'paragraphs': paragraphs
    }

def get_paragraphs(soup):
    """ Extract text from paragraphs """
    paragraphs = []
    for sec in soup.find_all('sec'):
        print(sec['id'])
        for i, p in enumerate(sec.find_all('p')):
            print(">>>>>", p)
            paragraphs.append({
                'section_id': sec['id'],
                'paragraph_id': "%s_%d" % (sec['id'], i),
                'raw_text': p.get_text()})
    return paragraphs

def build_features(filename):
    infos = extract_info(filename)
    spacy.load('en')
    for p in infos['paragraphs']:
        a = nlp(p)
    # return extract_info('nlp-blockchain-projects/data/processed/2017-09-distributed-data.cermxml')

@click.command()
# @click.argument('docs', type=click.File('r'))
@click.argument('input_filepath', default='data/processed', type=click.Path(exists=True))
def main(input_filepath):
    """ Runs data processing scripts to \turn raw data from (../raw) into
        cleaned data ready to be analyzed (saved in ../processed).
    """
    logger = logging.getLogger(__name__)
    logger.info('extracted features from processed data')
    logger.info(input_filepath)

    for x in os.listdir(input_filepath):
        x_ = os.path.join(input_filepath, x)
        if not os.path.isfile(x_) or not x_.endswith('xml'): continue
        build_features(x_)
    
if __name__ == "__main__":
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
