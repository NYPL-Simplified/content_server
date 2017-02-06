#!/usr/bin/env python
"""Assign genres for a number of FeedBooks subjects that weren't
previously included in classifier.TAGClassifier
"""
from nose.tools import set_trace
import os, sys
bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..")
sys.path.append(os.path.abspath(package_dir))

from core.model import (
    production_session,
    Subject,
)

try:
    _db = production_session()

    # Some of these tags end with whitespace, so fix that first.
    trailing = _db.query(Subject).filter(Subject.name.like(u"% "))
    for subject in trailing:
        subject.name = subject.name.strip()
    _db.commit()

    added_tags = [
        u'Archaeology',
        u'Arts',
        u'Baptist',
        u'Biographical',
        u'Christmas & Advent',
        u'Civil War Period (1850-1877)',
        u'Confucianism',
        u'Criticism & Theory',
        u'Customs & Traditions',
        u'Earth Sciences',
        u'Epistemology',
        u'Farce',
        u'Feminism & Feminist Theory',
        u'Government',
        u'Historical period',
        u'Historiography',
        u'Human Sexuality',
        u'Mechanics',
        u'Metaphysics',
        u'Ophthalmology',
        u'Oriental religions and wisdom',
        u'Personal Memoirs',
        u'Political',
        u'Pre-Confederation (to 1867)',
        u'Psychoanalysis',
        u'Reference',
        u'Revolutionary Period (1775-1800)',
        u'Sexuality',
        u'Short Stories',
        u'Taoism',
        u'Taoist',
        u'Tragicomedy',
        u'True story',
    ]

    subjects = _db.query(Subject).filter(
        Subject.identifier.like('FB%'),
        Subject.name.in_(added_tags))

    for subject in subjects:
        subject.assign_to_genre()
finally:
    _db.commit()
    _db.close()
