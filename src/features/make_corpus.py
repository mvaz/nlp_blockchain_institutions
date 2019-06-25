from bs4 import BeautifulSoup
import click
import logging
import os
from pathlib import Path
from dotenv import find_dotenv, load_dotenv


en = textacy.load_spacy_lang("en_core_web_lg")
patterns = [
    {"label": "ORG", "pattern": [{"lower": "european"}, {"lower": "central"}, {"lower": "bank"}]},
    {"label": "ORG", "pattern": [{"lower": "bank"}, {"lower": "of"}, {"lower": "japan"}]},
    {"label": "ORG", "pattern": [{"lower": "distributed"}, {"lower": "ledger"}, {"lower": "technology"}]},
    {"label": "ORG", "pattern": [{"lower": "consensus"}, {"lower": "mechanism"}]},
]



@click.command()
@click.argument('input_filepath', default='data/processed', type=click.Path(exists=True))
def main(input_filepath):
    """ Runs data processing scripts to \turn raw data from (../raw) into
        cleaned data ready to be analyzed (saved in ../processed).
    """
    logger = logging.getLogger(__name__)
    logger.info('extract features from processed data')
    logger.info(input_filepath)
    # from pprint import pprint
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
