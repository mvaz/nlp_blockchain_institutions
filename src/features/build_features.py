from bs4 import BeautifulSoup

import spacy
from spacy.matcher import PhraseMatcher
from spacy.tokens import Span


def get_title(soup):
    return soup.select('article-title')
    
# article
with open('data/processed/2017-09-distributed-data.cermxml') as f:
    xmla = f.read()
    soup = BeautifulSoup(xmla, features="lxml")
    print(get_title(soup))