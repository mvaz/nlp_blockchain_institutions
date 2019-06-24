from bs4 import BeautifulSoup
import click
import logging
import os
from pathlib import Path
from dotenv import find_dotenv, load_dotenv

import textacy
import textacy.keyterms
import ftfy

# Load English tokenizer, tagger, parser, NER and word vectors
en = textacy.load_spacy_lang("en_core_web_lg")
patterns = [
    {"label": "ORG", "pattern": [{"lower": "european"}, {"lower": "central"}, {"lower": "bank"}]},
    {"label": "ORG", "pattern": [{"lower": "bank"}, {"lower": "of"}, {"lower": "japan"}]},
    {"label": "ORG", "pattern": [{"lower": "distributed"}, {"lower": "ledger"}, {"lower": "technology"}]},
    {"label": "ORG", "pattern": [{"lower": "consensus"}, {"lower": "mechanism"}]},
]


# terms = (u"dlt", u"blockchain", u"distributed ledger technology", u"corda")
# entity_matcher = EntityMatcher(nlp, terms, "ANIMAL")

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
            logging.debug(p)
            paragraphs.append({
                'section_id': sec['id'],
                'paragraph_id': "%s_%d" % (sec['id'], i),
                'raw_text': p.get_text()})
    return paragraphs

def build_features(filename):
    infos = extract_info(filename)
    for p in infos['paragraphs']:
        yield scrub(p)
        
    # return extract_info('nlp-blockchain-projects/data/processed/2017-09-distributed-data.cermxml')

# Replace a token with "REDACTED" if it is a name
def replace_name_with_placeholder(token):
    if token.ent_iob != 0 and token.ent_type_ == "PERSON":
        return "[REDACTED] "
    else:
        return token.string

# Loop through all the entities in a document and check if they are names
def scrub(paragraph):
    txt = ftfy.fixes.fix_line_breaks(paragraph['raw_text'])
    txt = textacy.preprocess.normalize_whitespace(txt)
    text = textacy.preprocess_text(txt, lowercase=True, no_punct=False, fix_unicode=False, no_urls=True)
    doc = textacy.make_spacy_doc(text, lang=en)
    # doc = nlp(text['raw_text'])
    # for ent in doc.ents:
    #     ent.merge()
    # tokens = map(replace_name_with_placeholder, doc)
    return {
        'textrank': textacy.keyterms.textrank(doc, normalize="lemma", n_keyterms=10),
        'sgrank': textacy.keyterms.sgrank(doc, ngrams=(1, 2, 3, 4), normalize="lower", n_keyterms=0.1),
        'entities': list(textacy.extract.entities(doc))
    }


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
    from pprint import pprint

    for x in os.listdir(input_filepath):
        x_ = os.path.join(input_filepath, x)
        if not os.path.isfile(x_) or not x_.endswith('xml'): continue
        for feat in build_features(x_):
            logger.info(feat['textrank'])
        # break
    
if __name__ == "__main__":
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
