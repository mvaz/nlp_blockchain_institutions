from bs4 import BeautifulSoup
import click
import logging
import os
from pathlib import Path
from dotenv import find_dotenv, load_dotenv

import spacy
import textacy
from blockchain_dataset import BlockchainPapersDataset

# en = textacy.load_spacy_lang("en_core_web_lg")
# patterns = [
#     {"label": "ORG", "pattern": [{"lower": "european"}, {"lower": "central"}, {"lower": "bank"}]},
#     {"label": "ORG", "pattern": [{"lower": "bank"}, {"lower": "of"}, {"lower": "japan"}]},
#     {"label": "ORG", "pattern": [{"lower": "distributed"}, {"lower": "ledger"}, {"lower": "technology"}]},
#     {"label": "ORG", "pattern": [{"lower": "consensus"}, {"lower": "mechanism"}]},
# ]


entities = {
    'ECB': [
        [{'LOWER': 'ecb'}],
        [{'LOWER': 'european'}, {'LOWER': 'central'}, {'LOWER': 'bank'}]
    ],
    'BIS': [
        [{'LOWER': 'bis'}],
        [{'LOWER': 'bank'}, {'LOWER': 'of', 'OP': '?'}, {'LOWER': 'for', 'OP': '?'}, {'LOWER': 'international'}, {'LOWER': 'settlements'}]
    ],
    'BoE': [
        [{'LOWER': 'boe'}],
        [{'LOWER': 'bank'}, {'LOWER': 'of', 'OP': '?'}, {'LOWER': 'england'}]
    ],
    'BdF': [
        [{'LOWER': 'bdf'}],
        [{'LOWER': 'banque'}, {'LOWER': 'de', 'OP': '?'}, {'LOWER': 'france'}]
    ],
    'CPSS': [
        [{'LOWER': 'cpss'}],
        [{'LOWER': 'committee'}, {'LOWER': 'on', 'OP': '?'}, {'LOWER': 'payments'}, {'LOWER': 'and', 'OP': '?'},
         {'LOWER': 'settlement'}, {'LOWER': 'systems'}]
    ],
    'CPMI': [
        [{'LOWER': 'cpmi'}],
        [{'LOWER': 'committee'}, {'LOWER': 'on', 'OP': '?'}, {'LOWER': 'payments'}, {'LOWER': 'and', 'OP': '?'},
         {'LOWER': 'market'}, {'LOWER': 'infrastructures'}]
    ]
}

tech = {
    'DLT': [
        [{'LOWER': 'dlt'}],
        [{'LOWER': 'distributed'}, {'LOWER': 'ledger'}, {'LOWER': 'technology'}]
    ],
    'blokchain': [
        [{'LEMMA': 'blockchain'}]
    ],
    'consensus': [
        [{'LOWER': 'consensus'}, {'LOWER': 'mechanism'}],
        [{'LOWER': 'pbft'},]
    ],   
}

def _safe_add_pipe(lang, pipename, pipe):
    try:
        lang.remove_pipe(name='pipename')
    except ValueError as e:
        pass
    lang.add_pipe(pipe, name=pipename)
    return lang


def prepare_lang(lang="en_core_web_lg"):
    from spacy.pipeline import EntityRuler
    nlp = spacy.load('en_core_web_lg')
    ruler = EntityRuler(nlp)
    # define the pattern
    patterns = [dict(label = 'ORG', pattern=l) for k in entities for l in entities[k]]
    # add the pattern to the matcher object
    ruler.add_patterns(patterns)
    nlp = _safe_add_pipe(nlp, 'entities', ruler)

    ruler = EntityRuler(nlp)
    nlp.vocab.strings.add('TECH') 
    tech_patterns = [dict(label = 'TECH', pattern=l) for t in tech for l in tech[t]]
    ruler.add_patterns(tech_patterns)
    # add the matcher object as a new pipe to the model
    nlp = _safe_add_pipe(nlp, 'tech', ruler)

    return nlp
    

def create_corpus(lang="en_core_web_lg"):
    # nlp = en
    # component = entities.FinancialEntityRecognizer(nlp, entitites._financial_institutions)  # initialise component
    # en.add_pipe(component, before="ner")
    bpd = BlockchainPapersDataset()
    corpus = textacy.Corpus(lang, data=bpd.records())
    return corpus



@click.command()
@click.argument('corpus_filename', default='data/processed/corpus.acy', type=click.Path())
def main(corpus_filename):
    """ Runs data processing scripts to \turn raw data from (../raw) into
        cleaned data ready to be analyzed (saved in ../processed).
    """
    logger = logging.getLogger(__name__)
    logger.info('Create corpus')
    logger.info(os.path.abspath(corpus_filename))

    dlt_en = prepare_lang()
    corpus = create_corpus(lang=dlt_en)
    corpus.save(corpus_filename)


    
if __name__ == "__main__":
    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    # not used in this stub but often useful for finding various files
    project_dir = Path(__file__).resolve().parents[2]

    # find .env automagically by walking up directories until it's found, then
    # load up the .env entries as environment variables
    load_dotenv(find_dotenv())

    main()
