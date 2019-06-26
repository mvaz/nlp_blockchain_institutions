# -*- coding: utf-8 -*-
"""
Blockchain Papers Dataset
-------------------
A collection of papers from financial institutions about distributed ledger and
digital assets. Contains primarily texts (sections / paragraphs) in English-language.
Records include the following data:
    * ``text``: Full text of the paper / speech / news article.
    * ``title``: Title of the paper (optional).
    * ``entity``: Entity responsible for the publication.
    * ``date``: Date of publication.
    * ``url``: URL at which work can be found online.
    * ``filename``: Identifier of the publication within the dataset.
This dataset was compiled by David Mimno from the Oxford Text Archive and
stored in his GitHub repo to avoid unnecessary scraping of the OTA site. It is
downloaded from that repo, and excluding some light cleaning of its metadata,
is reproduced exactly here.
"""
from __future__ import absolute_import, division, print_function, unicode_literals

import io
import itertools
import logging
import os
import re

import textacy.compat as compat
# from .. import constants
import textacy.io as tio
from textacy.datasets import utils
from textacy.datasets.dataset import Dataset

LOGGER = logging.getLogger(__name__)

NAME = "blockchain_papers_dataset"
META = {
    "site_url": "https://github.com/mvaz/nlp_blockchain_institutions/",
    "description": (
        "A collection of papers from financial institutions about distributed ledger ",
        "and digital assets. Contains primarily texts in English-language."
    ),
}
DOWNLOAD_URL = "https://github.com/mvaz/nlp_blockchain_institutions/archive/blockchain_papers_dataset.zip"
DOWNLOAD_ROOT = "https://github.com/mvaz/nlp_blockchain_institutions/archive/"
DEFAULT_DATA_DIR = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "data", "interim"))

class BlockchainPapersDataset(Dataset):
    """
    Stream a collection of English-language papers from financial institutions about
    distributed ledger and digital assets from text files on disk,
    either as texts or text + metadata pairs.
    Download the data (one time only!), saving and extracting its contents to disk::
        >>> bpd = BlockchainPapersDataset()
        >>> bpd.download()
        >>> bpd.info
        {'name': 'blockchain_papers_dataset',
         'site_url': '
         ...'
        }
         
    Stream papers into a :class:`textacy.Corpus`::
        >>> textacy.Corpus("en", data=bpd.records(limit=5))
        Corpus(5 docs; 182289 tokens)
    Args:
        data_dir (str): Path to directory on disk under which dataset data is stored,
            i.e. ``/path/to/data_dir/blockchain_papers_dataset`` .
    Attributes:
        full_date_range (Tuple[str]): First and last dates for which works
            are available, each as an ISO-formatted string (YYYY-MM-DD).
        authors (Set[str]): Full names of all distinct authors included in this
            dataset, e.g. "Shakespeare, William".
    """

    full_date_range = ("0018-01-01", "1990-01-01")
    filenames = [
        "ecb.miptopical190604.en.pdf",
        "ecb.stella_project_report_september_2017.pdf",
        "stella_project_report_march_2018.pdf",
        "2017-09-distributed-data.pdf",
        "2018-10-25-blockbaster-final-report-data.pdf",
        "cryptoassets_taskforce_final_report_final_web.pdf",
        "monetary-financing-with-interest-bearing-money.pdf",
        "central-bank-digital-currencies-design-principles-and-balance-sheet-implications.pdf",
        "BOT_DLT_Scriptless_Bond.pdf",
        "ecbop172.en.pdf",
        "d157.pdf",
        "d174.pdf",
        "work698.pdf",
        "r_qt1709f.pdf",
        "r180313a.pdf",
        "SDN1808.pdf",
        "the-economics-of-distributed-ledger-technology-for-securities-settlement.pdf",
        "2016095pap.pdf",
        "CBDC_Brazil_R3.pdf",
        "bnm_gov_my.pdf",
        #   "bnm_gov_my.pdf",
        "the-riksbanks-e-krona-project-report-2.pdf",
        "the-riksbanks-e-krona-project-report-1.pdf"
    ]

    institutions = [
        "European Central Bank",
        "Bundesbank",
        "Deutsche Börse",
        "UK Government",
        "Bank of England",
        "Bank of Thailand",
        "Bank of International Settlements",
        "International Monetary Fund",
        "Federal Reserve Board",
        "R3",
        "Central Bank of Malaysia",
        #   "The Institute and Faculty of Actuaries (IFoA)",
        "Sveriges Riksbank"
    ]


    def __init__(self, data_dir=os.path.join(DEFAULT_DATA_DIR, NAME)):
        super(BlockchainPapersDataset, self).__init__(NAME, meta=META)
        self.data_dir = data_dir
        self._filename = "dataset.json"
        self._filepath = os.path.join(self.data_dir, self._filename)
        # self._metadata_filepath = os.path.join(self.data_dir, "master", "metadata.tsv")
        self._metadata = META

    @property
    def filepath(self):
        """
        str: Full path on disk for BlockchainPapers data as compressed json file.
            ``None`` if file is not found, e.g. has not yet been downloaded.
        """
        if os.path.isfile(self._filepath):
            return self._filepath
        else:
            return None
    
    @property
    def metadata(self):
        """
        Dict[str, dict]
        """
        if not self._metadata:
            try:
                self._metadata = self._load_and_parse_metadata()
            except OSError as e:
                LOGGER.error(e)
        return self._metadata
    
    def download(self, force=False):
        """
        Download the data as a Python version-specific compressed json file and
        save it to disk under the ``data_dir`` directory.
        Args:
            force (bool): If True, download the dataset, even if it already
                exists on disk under ``data_dir``.
        """
        release_tag = "blockchain_papers_dataset{py_version}_v{data_version}".format(
            py_version=2 if compat.PY2 else 3,
            data_version=1.0,
        )
        url = compat.urljoin(DOWNLOAD_ROOT, release_tag + "/" + self._filename)
        filepath = utils.download_file(
            url,
            filename=self._filename,
            dirpath=self.data_dir,
            force=force,
        )

    def __iter__(self):
        if not os.path.isfile(self._filepath):
            raise OSError(
                "dataset file {} not found;\n"
                "has the dataset been downloaded yet?".format(self._filepath)
            )
        mode = "rb" if compat.PY2 else "rt"  # TODO: check this
        for record in tio.read_json(self._filepath, mode=mode, lines=True):
            yield record
    
    # FIXME
    def _get_filters( 
        self,
        filename,
        institution,
        date_range,
        min_len,
    ):
        filters = []
        if min_len is not None:
            if min_len < 1:
                raise ValueError("`min_len` must be at least 1")
            filters.append(
                lambda record: len(record.get("text", "")) >= min_len
            )
        if date_range is not None:
            date_range = utils.validate_and_clip_range_filter(
                date_range, self.full_date_range, val_type=compat.string_types)
            filters.append(
                lambda record: (
                    record.get("date")
                    and date_range[0] <= record["date"] < date_range[1]
                )
            )
        if filename is not None:
            filename = utils.validate_set_member_filter(
                filename, compat.string_types, valid_vals=self.filenames)
            filters.append(lambda record: record.get("filename") in filename)
        if institution is not None:
            institution = utils.validate_set_member_filter(
                institution, compat.string_types, valid_vals=self.institutions)
            filters.append(lambda record: record.get("institution") in institution)
        return filters


    def _filtered_iter(self, filters):
        if filters:
            for record in self:
                if all(filter_(record) for filter_ in filters):
                    yield record
        else:
            for record in self:
                yield record


    def texts(self, filename=None, institution=None, date_range=None, min_len=None, limit=None):
        """
        Iterate over works in this dataset, optionally filtering by a variety
        of metadata and/or text length, and yield texts only.
        Args:
            insitution (str or Set[str]): Filter texts by the institutions' name.
                For multiple values (Set[str]), ANY rather than ALL of the authors
                must be found among a given works's authors.
            date_range (List[str] or Tuple[str]): Filter texts by the date on
                which it was published; both start and end date must be specified,
                but a null value for either will be replaced by the min/max date
                available in the dataset.
            min_len (int): Filter texts by the length (number of characters)
                of their text content.
            limit (int): Return no more than ``limit`` texts.
        Yields:
            str: Text of the next work in dataset passing all filters.
        Raises:
            ValueError: If any filtering options are invalid.
        """
        filters = self._get_filters(filename, institution, date_range, min_len)
        for record in itertools.islice(self._filtered_iter(filters), limit):
            yield record["text"]

    def records(self, filename=None, institution=None, date_range=None, min_len=None, limit=None):
        """
        Iterate over works in this dataset, optionally filtering by a variety
        of metadata and/or text length, and yield text + metadata pairs.
        Args:
            author (str or Set[str]): Filter records by the authors' name;
                see :attr:`OxfordTextArchive.authors`.
            date_range (List[str] or Tuple[str]): Filter records by the date on
                which it was published; both start and end date must be specified,
                but a null value for either will be replaced by the min/max date
                available in the dataset.
            min_len (int): Filter records by the length (number of characters)
                of their text content.
            limit (int): Yield no more than ``limit`` records.
        Yields:
            str: Text of the next work in dataset passing all filters.
            dict: Metadata of the next work in dataset passing all filters.
        Raises:
            ValueError: If any filtering options are invalid.
        """
        filters = self._get_filters(filename, institution, date_range, min_len)
        for record in itertools.islice(self._filtered_iter(filters), limit):
            yield record.pop("text"), record
