#!/usr/bin/env python
"""Fixes thumbnails for Standard Ebooks that end in '.svg.png'
and thus render incorrectly (and largely invisibly)
"""
from nose.tools import set_trace
import os, sys
bin_dir = os.path.split(__file__)[0]
package_dir = os.path.join(bin_dir, "..")
sys.path.append(os.path.abspath(package_dir))

from sqlalchemy import or_

from core.external_search import DummyExternalSearchIndex
from core.model import (
    DataSource,
    Edition,
    Identifier,
    Hyperlink,
    LicensePool,
    PresentationCalculationPolicy,
    Representation,
    Resource,
    Work,
    production_session,
)

BROKEN_IMAGE_URLS = [
  u'http://book-covers.nypl.org/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/abraham-merritt/the-moon-pool/cover.svg.png',
  u'http://book-covers.nypl.org/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/alfred-lord-tennyson/idylls-of-the-king/gustave-dore/cover.svg.png',
  u'http://book-covers.nypl.org/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/algis-budrys/short-fiction/cover.svg.png',
  u'http://book-covers.nypl.org/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/anton-chekhov/the-duel/constance-garnett/cover.svg.png',
  u'http://book-covers.nypl.org/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/apsley-cherry-garrard/the-worst-journey-in-the-world/cover.svg.png',
  u'http://book-covers.nypl.org/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/charles-dickens/david-copperfield/cover.svg.png',
  u'http://book-covers.nypl.org/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/edgar-rice-burroughs/a-princess-of-mars/frank-e-schoonover/cover.svg.png',
  u'http://book-covers.nypl.org/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/emile-zola/his-masterpiece/ernest-alfred-vizetelly/cover.svg.png',
  u'http://book-covers.nypl.org/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/fritz-leiber/the-big-time/cover.svg.png',
  u'http://book-covers.nypl.org/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/g-k-chesterton/the-innocence-of-father-brown/cover.svg.png',
  u'http://book-covers.nypl.org/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/g-k-chesterton/the-man-who-was-thursday/cover.svg.png',
  u'http://book-covers.nypl.org/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/h-beam-piper/space-viking/cover.svg.png',
  u'http://book-covers.nypl.org/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/h-g-wells/the-time-machine/cover.svg.png',
  u'http://book-covers.nypl.org/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/henry-david-thoreau/walden/cover.svg.png',
  u'http://book-covers.nypl.org/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/hermann-hesse/siddhartha/gunther-olesch-anke-dreher-amy-coulter-stefan-langer-semyon-chaichenets/cover.svg.png',
  u'http://book-covers.nypl.org/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/jack-london/the-call-of-the-wild/cover.svg.png',
  u'http://book-covers.nypl.org/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/jack-london/white-fang/cover.svg.png',
  u'http://book-covers.nypl.org/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/james-fenimore-cooper/the-last-of-the-mohicans/cover.svg.png',
  u'http://book-covers.nypl.org/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/james-joyce/dubliners/cover.svg.png',
  u'http://book-covers.nypl.org/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/jane-austen/pride-and-prejudice/cover.svg.png',
  u'http://book-covers.nypl.org/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/jerome-k-jerome/three-men-in-a-boat/cover.svg.png',
  u'http://book-covers.nypl.org/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/jules-verne/around-the-world-in-eighty-days/george-makepeace-towle/cover.svg.png',
  u'http://book-covers.nypl.org/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/jules-verne/the-mysterious-island/stephen-w-white/cover.svg.png',
  u'http://book-covers.nypl.org/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/laozi/tao-te-ching/james-legge/cover.svg.png',
  u'http://book-covers.nypl.org/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/leo-tolstoy/a-confession/louise-maude/cover.svg.png',
  u'http://book-covers.nypl.org/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/lewis-carroll/alices-adventures-in-wonderland/cover.svg.png',
  u'http://book-covers.nypl.org/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/lord-dunsany/the-book-of-wonder/sidney-h-sime/cover.svg.png',
  u'http://book-covers.nypl.org/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/marcus-aurelius/meditations/george-long/cover.svg.png',
  u'http://book-covers.nypl.org/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/mark-twain/the-adventures-of-huckleberry-finn/cover.svg.png',
  u'http://book-covers.nypl.org/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/mary-shelley/frankenstein/cover.svg.png',
  u'http://book-covers.nypl.org/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/omar-khayyam/the-rubaiyat-of-omar-khayyam/edward-fitzgerald/edmund-dulac/cover.svg.png',
  u'http://book-covers.nypl.org/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/oscar-wilde/the-picture-of-dorian-gray/cover.svg.png',
  u'http://book-covers.nypl.org/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/p-g-wodehouse/right-ho-jeeves/cover.svg.png',
  u'http://book-covers.nypl.org/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/p-t-barnum/the-art-of-money-getting/cover.svg.png',
  u'http://book-covers.nypl.org/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/philip-k-dick/short-fiction/cover.svg.png',
  u'http://book-covers.nypl.org/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/robert-frost/north-of-boston/cover.svg.png',
  u'http://book-covers.nypl.org/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/robert-louis-stevenson/treasure-island/milo-winter/cover.svg.png',
  u'http://book-covers.nypl.org/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/rudyard-kipling/the-jungle-book/cover.svg.png',
  u'http://book-covers.nypl.org/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/upton-sinclair/the-jungle/cover.svg.png',
  u'http://book-covers.nypl.org/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/w-w-jacobs/the-lady-of-the-barge/maurice-greiffenhagen/cover.svg.png',
  u'http://book-covers.nypl.org/scaled/300/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/abraham-merritt/the-moon-pool/cover.svg.png',
  u'http://book-covers.nypl.org/scaled/300/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/alfred-lord-tennyson/idylls-of-the-king/gustave-dore/cover.svg.png',
  u'http://book-covers.nypl.org/scaled/300/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/algis-budrys/short-fiction/cover.svg.png',
  u'http://book-covers.nypl.org/scaled/300/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/anton-chekhov/the-duel/constance-garnett/cover.svg.png',
  u'http://book-covers.nypl.org/scaled/300/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/apsley-cherry-garrard/the-worst-journey-in-the-world/cover.svg.png',
  u'http://book-covers.nypl.org/scaled/300/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/charles-dickens/david-copperfield/cover.svg.png',
  u'http://book-covers.nypl.org/scaled/300/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/edgar-rice-burroughs/a-princess-of-mars/frank-e-schoonover/cover.svg.png',
  u'http://book-covers.nypl.org/scaled/300/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/emile-zola/his-masterpiece/ernest-alfred-vizetelly/cover.svg.png',
  u'http://book-covers.nypl.org/scaled/300/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/fritz-leiber/the-big-time/cover.svg.png',
  u'http://book-covers.nypl.org/scaled/300/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/g-k-chesterton/the-innocence-of-father-brown/cover.svg.png',
  u'http://book-covers.nypl.org/scaled/300/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/g-k-chesterton/the-man-who-was-thursday/cover.svg.png',
  u'http://book-covers.nypl.org/scaled/300/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/h-beam-piper/space-viking/cover.svg.png',
  u'http://book-covers.nypl.org/scaled/300/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/h-g-wells/the-time-machine/cover.svg.png',
  u'http://book-covers.nypl.org/scaled/300/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/henry-david-thoreau/walden/cover.svg.png',
  u'http://book-covers.nypl.org/scaled/300/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/hermann-hesse/siddhartha/gunther-olesch-anke-dreher-amy-coulter-stefan-langer-semyon-chaichenets/cover.svg.png',
  u'http://book-covers.nypl.org/scaled/300/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/jack-london/the-call-of-the-wild/cover.svg.png',
  u'http://book-covers.nypl.org/scaled/300/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/jack-london/white-fang/cover.svg.png',
  u'http://book-covers.nypl.org/scaled/300/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/james-fenimore-cooper/the-last-of-the-mohicans/cover.svg.png',
  u'http://book-covers.nypl.org/scaled/300/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/james-joyce/dubliners/cover.svg.png',
  u'http://book-covers.nypl.org/scaled/300/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/jane-austen/pride-and-prejudice/cover.svg.png',
  u'http://book-covers.nypl.org/scaled/300/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/jerome-k-jerome/three-men-in-a-boat/cover.svg.png',
  u'http://book-covers.nypl.org/scaled/300/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/jules-verne/around-the-world-in-eighty-days/george-makepeace-towle/cover.svg.png',
  u'http://book-covers.nypl.org/scaled/300/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/jules-verne/the-mysterious-island/stephen-w-white/cover.svg.png',
  u'http://book-covers.nypl.org/scaled/300/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/laozi/tao-te-ching/james-legge/cover.svg.png',
  u'http://book-covers.nypl.org/scaled/300/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/leo-tolstoy/a-confession/louise-maude/cover.svg.png',
  u'http://book-covers.nypl.org/scaled/300/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/lewis-carroll/alices-adventures-in-wonderland/cover.svg.png',
  u'http://book-covers.nypl.org/scaled/300/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/marcus-aurelius/meditations/george-long/cover.svg.png',
  u'http://book-covers.nypl.org/scaled/300/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/mark-twain/the-adventures-of-huckleberry-finn/cover.svg.png',
  u'http://book-covers.nypl.org/scaled/300/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/mary-shelley/frankenstein/cover.svg.png',
  u'http://book-covers.nypl.org/scaled/300/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/omar-khayyam/the-rubaiyat-of-omar-khayyam/edward-fitzgerald/edmund-dulac/cover.svg.png',
  u'http://book-covers.nypl.org/scaled/300/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/p-g-wodehouse/right-ho-jeeves/cover.svg.png',
  u'http://book-covers.nypl.org/scaled/300/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/p-t-barnum/the-art-of-money-getting/cover.svg.png',
  u'http://book-covers.nypl.org/scaled/300/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/philip-k-dick/short-fiction/cover.svg.png',
  u'http://book-covers.nypl.org/scaled/300/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/robert-frost/north-of-boston/cover.svg.png',
  u'http://book-covers.nypl.org/scaled/300/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/robert-louis-stevenson/treasure-island/milo-winter/cover.svg.png',
  u'http://book-covers.nypl.org/scaled/300/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/rudyard-kipling/the-jungle-book/cover.svg.png',
  u'http://book-covers.nypl.org/scaled/300/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/upton-sinclair/the-jungle/cover.svg.png',
  u'http://book-covers.nypl.org/scaled/300/Standard%20Ebooks/URI/https%3A//standardebooks.org/ebooks/w-w-jacobs/the-lady-of-the-barge/maurice-greiffenhagen/cover.svg.png'
]

BROKEN_EXTENSION = u'.svg.png'

def explain(edition, representation, work):
    print "** %s by %s **" % (edition.author, edition.title)
    print "\tCOVER: %s" % edition.cover_full_url
    print "\tTHUMB: %s" % edition.cover_thumbnail_url
    print "\tREP: %s" % representation.mirror_url
    print "\n"

try:
    _db = production_session()

    LIKE_EXTENSION = '%'+BROKEN_EXTENSION+'%'
    qu = _db.query(Edition, Representation, Work)\
        .join(Edition.primary_identifier).join(Identifier.links)\
        .join(Hyperlink.resource).join(Resource.representation)\
        .join(Identifier.licensed_through).join(LicensePool.data_source)\
        .join(Edition.work).filter(
            Hyperlink.rel.in_([Hyperlink.IMAGE, Hyperlink.THUMBNAIL_IMAGE]),
            Representation.mirror_url.like(LIKE_EXTENSION),
            or_(
                Work.verbose_opds_entry.like(LIKE_EXTENSION),
                Work.simple_opds_entry.like(LIKE_EXTENSION)),
            or_(
                Representation.mirror_url.in_(BROKEN_IMAGE_URLS),
                Edition.cover_full_url.in_(BROKEN_IMAGE_URLS),
                Edition.cover_thumbnail_url.in_(BROKEN_IMAGE_URLS)))

    count = 0
    policy = PresentationCalculationPolicy(regenerate_opds_entries=True)
    fake_search = DummyExternalSearchIndex()
    for (edition, representation, work) in qu:
        explain(edition, representation, work)

        # It might just be an issue with a cached OPDS entry and not
        # the edition at all, so first check to see if the extension
        # is in either image URL.
        opds_problem = not (edition.cover_full_url.endswith(BROKEN_EXTENSION)
            or edition.cover_thumbnail_url.endswith(BROKEN_EXTENSION))

        if not opds_problem:
            # The problem extends past the OPDS entries to the
            # edition image URLs. These URLs haven't been replaced
            # by '.jpg' images and those images likely don't exist.
            #
            # Instead, point to an existing '.png' file -- without the
            # '.svg' interruption.
            for url_type in ['cover_full_url', 'cover_thumbnail_url']:
                url = getattr(edition, url_type)
                if url.endswith(BROKEN_EXTENSION):
                    correct_image_url = url.replace(BROKEN_EXTENSION, '.png')
                    setattr(edition, url_type, correct_image_url)
                    representation.mirror_url = correct_image_url

        # Recreate the OPDS entries.
        work.calculate_presentation(policy=policy, search_index_client=fake_search)

        # Confirm that the problem files are no longer being linked
        # in the OPDS entries.
        present = (BROKEN_EXTENSION in work.simple_opds_entry
                or BROKEN_EXTENSION in work.verbose_opds_entry)
        if not present:
            print "The %s file has been removed from all OPDS entries." % BROKEN_EXTENSION
        else:
            print "ERROR: The %s file could not be removed: %r." % (BROKEN_EXTENSION, work)

        count += 1

    print "Updated %i Works" % count
finally:
    _db.commit()
    _db.close()
