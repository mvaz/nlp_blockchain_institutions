from bs4 import BeautifulSoup
import click
import logging
import os
from pathlib import Path
from dotenv import find_dotenv, load_dotenv

import textacy
import textacy.keyterms
import ftfy

import entities

# Load English tokenizer, tagger, parser, NER and word vectors
en = textacy.load_spacy_lang("en_core_web_lg")
patterns = [
    {"label": "ORG", "pattern": [{"lower": "european"}, {"lower": "central"}, {"lower": "bank"}]},
    {"label": "ORG", "pattern": [{"lower": "bank"}, {"lower": "of"}, {"lower": "japan"}]},
    {"label": "ORG", "pattern": [{"lower": "distributed"}, {"lower": "ledger"}, {"lower": "technology"}]},
    {"label": "ORG", "pattern": [{"lower": "consensus"}, {"lower": "mechanism"}]},
]



# Replace a token with "REDACTED" if it is a name
def replace_name_with_placeholder(token):
    if token.ent_iob != 0 and token.ent_type_ == "PERSON":
        return "[REDACTED] "
    else:
        return token.string

# Loop through all the entities in a document and check if they are names
def scrub(paragraph):
    txt = paragraph['raw_text']
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
    logger.info('extract features from processed data')
    logger.info(input_filepath)
    from pprint import pprint
    # nlp = en
    # component = entities.FinancialEntityRecognizer(nlp, entitites._financial_institutions)  # initialise component
    # en.add_pipe(component, before="ner")

    
if __name__ == "__main__":
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
