from spacy.matcher import PhraseMatcher
from spacy.pipeline import EntityRuler
from spacy.tokens import Doc, Span, Token

_financial_institutions = [str.lower (x) for x in [
    "ECB", "European Central Bank",
    "BOJ", "Bank of Japan",
    "Bank of England", "BoE",
    "Banque de France", "BdF",
    "JP Morgan",]]



_financial_terms = ("central banks", "commercial banks", "exchanges", "clearing house", "CSD")

_technology_terms = (u"dlt", u"blockchain", u"distributed ledger technology", u"corda",
"hyperledger fabric", "hyperledger", "fabric", "ethereum", "bitcoin", "stellar",
"bitstamp", "bitfinex", )
# _tech_entity_matcher = EntityMatcher(nlp, terms, "ANIMAL")

def financial_matcher_factory(nlp):
    matcher = PhraseMatcher(nlp.vocab, attr='LOWER')
    patterns = [nlp.make_doc(text) for text in _financial_terms]
    matcher.add("Phrase Matching", None, *patterns)
    return matcher


def tech_matcher_factory(nlp):
    matcher = PhraseMatcher(nlp.vocab, attr='LOWER')
    patterns = [nlp.make_doc(text) for text in _technology_terms]
    matcher.add("Phrase Matching", None, *patterns)
    return matcher


class FinancialEntityRecognizer(object):
    """Example of a spaCy v2.0 pipeline component that sets entity annotations
    based on list of single or multiple-word company names. Companies are
    labelled as ORG and their spans are merged into one token. Additionally,
    ._.has_tech_org and ._.is_tech_org is set on the Doc/Span and Token
    respectively."""

    name = "financial_institutions"  # component name, will show up in the pipeline

    def __init__(self, nlp, companies=_financial_institutions, label="ORG"):
        """Initialise the pipeline component. The shared nlp instance is used
        to initialise the matcher with the shared vocab, get the label ID and
        generate Doc objects as phrase match patterns.
        """
        self.label = nlp.vocab.strings[label]  # get entity label ID

        # Set up the PhraseMatcher – it can now take Doc objects as patterns,
        # so even if the list of companies is long, it's very efficient
        patterns = [nlp(org) for org in companies]
        self.matcher = PhraseMatcher(nlp.vocab)
        self.matcher.add("FINANCIAL_ORGS", None, *patterns)

        # Register attribute on the Token. We'll be overwriting this based on
        # the matches, so we're only setting a default value, not a getter.
        Token.set_extension("is_financial_org", default=False)

        # Register attributes on Doc and Span via a getter that checks if one of
        # the contained tokens is set to is_tech_org == True.
        Doc.set_extension("has_financial_org", getter=self.has_financial_org)
        Span.set_extension("has_financial_org", getter=self.has_financial_org)

    def __call__(self, doc):
        """Apply the pipeline component on a Doc object and modify it if matches
        are found. Return the Doc, so it can be processed by the next component
        in the pipeline, if available.
        """
        matches = self.matcher(doc)
        spans = []  # keep the spans for later so we can merge them afterwards
        for _, start, end in matches:
            # Generate Span representing the entity & set label
            entity = Span(doc, start, end, label=self.label)
            spans.append(entity)
            # Set custom attribute on each token of the entity
            for token in entity:
                token._.set("is_financial_org", True)
            # Overwrite doc.ents and add entity – be careful not to replace!
            doc.ents = list(doc.ents) + [entity]
        for span in spans:
            # Iterate over all spans and merge them into one token. This is done
            # after setting the entities – otherwise, it would cause mismatched
            # indices!
            span.merge()
        return doc  # don't forget to return the Doc!

    def has_financial_org(self, tokens):
        """Getter for Doc and Span attributes. Returns True if one of the tokens
        is a financial org. Since the getter is only called when we access the
        attribute, we can refer to the Token's 'is_financial_org' attribute here,
        which is already set in the processing step."""
        return any([t._.get("is_financial_org") for t in tokens])
