import datetime
import feedparser
from nose.tools import set_trace
from core.opds import OPDSFeed
from core.opds_import import (
    OPDSImporterWithS3Mirror,
    OPDSXMLParser,
)
from core.model import (
    DataSource,
    Hyperlink,
    Resource,
    Representation,
    RightsStatus,
)

class FeedbooksOPDSImporter(OPDSImporterWithS3Mirror):

    DATA_SOURCE_NAME = "FeedBooks"
    THIRTY_DAYS = datetime.timedelta(days=30)

    def __init__(self, _db, *args, **kwargs):
        super(FeedbooksOPDSImporter, self).__init__(
            _db, self.DATA_SOURCE_NAME, *args, **kwargs
        )
    
    def extract_feed_data(self, feed, feed_url=None):
        metadata, failures = super(FeedbooksOPDSImporter, self).extract_feed_data(
            feed, feed_url
        )
        for id, m in metadata.items():
            self.improve_description(id, m)
        return metadata, failures

    @classmethod
    def rights_uri_from_feedparser_entry(cls, entry):
        """Determine the URI that best encapsulates the rights status of
        the downloads associated with this book.

        This answer is not reliable because we don't actually have
        access to dcterms:issued, but we do the best we can. This will
        be called by _detail_for_feedparser_entry and fixed by
        _detail_for_elementtree_entry.
        """
        rights = entry.get('rights', "")
        source = entry.get('dcterms_source', '')
        issued = entry.get('dcterms_issued', None) # :(
        return RehostingPolicy.rights_uri(rights, source, issued)

    @classmethod
    def rights_uri_from_entry_tag(cls, entry):
        rights = OPDSXMLParser._xpath1(entry, 'atom:rights')
        if rights is not None:
            rights = rights.text
        source = OPDSXMLParser._xpath1(entry, 'dcterms:source')
        if source is not None:
            source = source.text
        publication_year = OPDSXMLParser._xpath1(entry, 'dcterms:issued')
        if publication_year is not None:
            publication_year = publication_year.text
        return RehostingPolicy.rights_uri(rights, source, publication_year)

    @classmethod
    def _detail_for_elementtree_entry(cls, parser, entry_tag, feed_url=None,
                                      feedparser_detail=None):
        """Correct the default rights URI in the circulation data obtained
        previously for this entry.

        We have to correct the data after the fact instead of getting
        it right within rights_uri_from_feedparser_entry, because
        dcterms:issued (which we use to determine whether a work is
        public domain in the United States) is not available through
        Feedparser.
        """
        rights_uri = cls.rights_uri_from_entry_tag(entry_tag)
        circulation = feedparser_detail.setdefault('circulation', {})
        circulation['default_rights_uri'] = rights_uri            
        return OPDSImporterWithS3Mirror._detail_for_elementtree_entry(
            cls, parser, entry_tag, feed_url,
            feedparser_detail
        )
    
    @classmethod
    def make_link_data(cls, rel, href=None, media_type=None, rights_uri=None,
                       content=None):
        """Turn basic link information into a LinkData object.

        FeedBooks puts open-access content behind generic 'acquisition'
        links. We want to treat them as open-access links.

        At the same time, we _don't_ want to mirror content we don't have
        the right to rehost.
        """
        if rel==Hyperlink.GENERIC_OPDS_ACQUISITION and media_type:
            rel = Hyperlink.OPEN_ACCESS_DOWNLOAD
        return OPDSImporterWithS3Mirror.make_link_data(
            rel, href, media_type, rights_uri, content
        )
    
    def improve_description(self, id, metadata):
        """Improve the description associated with a book,
        if possible.

        This involves fetching an alternate OPDS entry that might
        contain more detailed descriptions than those available in the
        main feed.
        """
        alternate_links = []
        existing_descriptions = []
        everything_except_descriptions = []
        for x in metadata.links:
            if (x.rel == Hyperlink.ALTERNATE and x.href
                and x.media_type == OPDSFeed.ENTRY_TYPE):
                alternate_links.append(x)
            if x.rel == Hyperlink.DESCRIPTION:
                existing_descriptions.append((x.media_type, x.content))
            elif x.rel == Hyperlink.GENERIC_OPDS_ACQUISITION:
                # This feed contains open-access links
                # coded as generic acquisition links.
                x.rel = Hyperlink.OPEN_ACCESS_DOWNLOAD
            else:
                everything_except_descriptions.append(x)

        better_descriptions = []
        for alternate_link in alternate_links:
            # There should only be one alternate link, but we'll keep
            # processing them until we get a good description.

            # Fetch the alternate entry.
            print alternate_link.href
            representation, is_new = Representation.get(
                self._db, alternate_link.href, max_age=self.THIRTY_DAYS,
                do_get=self.http_get
            )

            if representation.status_code != 200:
                continue
            
            # Parse the alternate entry with feedparser and run it through
            # data_detail_for_feedparser_entry().
            parsed = feedparser.parse(representation.content)
            if len(parsed['entries']) != 1:
                # This is supposed to be a single entry, and it's not.
                continue
            [entry] = parsed['entries']
            data_source = DataSource.lookup(
                self._db, self.data_source_name, autocreate=True,
                offers_licenses=True
            )
            detail_id, new_detail, failure = self.data_detail_for_feedparser_entry(
                entry, data_source
            )
            if failure:
                # There was a problem parsing the entry.
                self.log.error(failure.exception)
                continue
            
            # TODO: Ideally we could verify that detail_id == id, but
            # right now they are always different -- one is an HTTPS
            # URI and one is an HTTP URI. So we omit this step and
            # assume the documents at both ends of the 'alternate'
            # link identify the same resource.

            # Find any descriptions present in the alternate view which
            # are not present in the original.
            new_descriptions = [
                x for x in new_detail['links']
                if x.rel == Hyperlink.DESCRIPTION
                and (x.media_type, x.content) not in existing_descriptions
            ]

            if new_descriptions:
                # Replace old descriptions with new descriptions.
                metadata.links = (
                    everything_except_descriptions + new_descriptions
                )
                break

        return metadata


class RehostingPolicy(object):
    """Determining the precise copyright status of the underlying text
    is not directly useful, because Feedbooks has made derivative
    works and relicensed under CC-BY-NC. So that's going to be the
    license: CC-BY-NC.
    
    Except it's not that simple. There are two complications.
    
    1. Feedbooks is located in France, and NYPL's open-access
    content server is hosted in the US. We can't host a CC-BY-NC
    book if it's derived from a work that's still under US
    copyright. We must decide whether or not to accept a book in the
    first place based on the copyright status of the underlying
    text.
    
    2. Some CC licenses are more restrictive (on the creators of
    derivative works) than CC-BY-NC. Feedbooks has no authority to
    relicense these books, so they need to be preserved.

    This class encapsulates the logic necessary to make this decision.
    """
    
    PUBLIC_DOMAIN_CUTOFF = 1923    

    # These are the licenses that need to be preserved.
    RIGHTS_DICT = {    
        "Attribution Share Alike (cc by-sa)" : RightsStatus.CC_BY_SA,
        "Attribution Non-Commercial No Derivatives (cc by-nc-nd)" : RightsStatus.CC_BY_NC_ND,
        "Attribution Non-Commercial Share Alike (cc by-nc-sa)" : RightsStatus.CC_BY_NC_SA,
    }
    
    # Feedbooks rights statuses indicating books that can be rehosted
    # in the US.
    CAN_REHOST_IN_US = set([
        "This work was published before 1923 and is in the public domain in the USA only.",
        "This work is available for countries where copyright is Life+70 and in the USA.",
        'This work is available for countries where copyright is Life+50 or in the USA (published before 1923).',
        "Attribution (cc by)",
        "Attribution Non-Commercial (cc by-nc)",

        "Attribution Share Alike (cc by-sa)",
        "Attribution Non-Commercial No Derivatives (cc by-nc-nd)",
        "Attribution Non-Commercial Share Alike (cc by-nc-sa)",
    ])

    RIGHTS_UNKNOWN = "Please read the legal notice included in this e-book and/or check the copyright status in your country."
    
    # These websites are hosted in the US and specialize in
    # open-access content. We will accept all FeedBooks titles taken
    # from these sites, even post-1923 titles.
    US_SITES = set([
        "archive.org",
        "craphound.com",
        "en.wikipedia.org",
        "en.wikisource.org",
        "futurismic.com",
        "gutenberg.org",
        "project gutenberg",
        "shakespeare.mit.edu",
    ])

    @classmethod
    def rights_uri(cls, rights, source, publication_year):
        if publication_year and isinstance(publication_year, basestring):
            publication_year = int(publication_year)

        can_rehost = cls.can_rehost_us(rights, source, publication_year)
        if can_rehost is False:
            # We believe this book is still under copyright in the US
            # and we should not rehost it.
            return RightsStatus.IN_COPYRIGHT

        if can_rehost is None:
            # We don't have enough information to know whether the book
            # is under copyright in the US. We should not host it.
            return RightsStatus.UNKNOWN

        if rights in cls.RIGHTS_DICT:
            # The CC license of the underlying text means it cannot be
            # relicensed CC-BY-NC.
            return cls.RIGHTS_DICT[rights]

        # The default license as per our agreement with FeedBooks.
        return RightsStatus.CC_BY_NC
    
    @classmethod
    def can_rehost_us(cls, rights, source, publication_year):
        """Can we rehost this book on a US server?

        :param rights: What FeedBooks says about the public domain status
        of the book.

        :param source: Where FeedBooks got the book.

        :param publication_year: When the text was originally published.

        :return: True if we can rehost in the US, False if we can't,
        None if we're not sure. The distinction between False and None
        is only useful when making lists of books that need to have
        their rights status manually investigated.
        """    
        if publication_year and publication_year < cls.PUBLIC_DOMAIN_CUTOFF:
            # We will rehost anything published prior to 1923, no
            # matter where it came from.
            return True
        
        if rights in cls.CAN_REHOST_IN_US:
            # This book's FeedBooks rights statement explicitly marks
            # it as one that can be rehosted in the US.
            return True
            
        # The rights statement isn't especially helpful, but maybe we
        # can make a determination based on where Feedbooks got the
        # book from.
        source = (source or "").lower()

        if any(site in source for site in cls.US_SITES):
            # This book originally came from a US-hosted site that
            # specializes in open-access books, so we must be able
            # to rehost it.
            return True

        if source in ('wikisource', 'gutenberg'):
            # Presumably en.wikisource and Project Gutenberg US.  We
            # special case these to avoid confusing the US versions of
            # these sites with other countries'.
            return True
                
        # And we special-case this one to avoid confusing Australian
        # Project Gutenberg with US Project Gutenberg.
        if ('gutenberg.net' in source and not 'gutenberg.net.au' in source):
            return True
        
        # Unless one of the above conditions is met, we must assume
        # the book cannot be rehosted in the US.
        if rights == cls.RIGHTS_UNKNOWN:
            # To be on the safe side we're not going to host this
            # book, but we actually don't know that it's unhostable.
            return None

        # In this case we're pretty sure. The rights status indicates
        # some kind of general incompatible restriction (such as
        # Life+70) and it's not a pre-1923 book.
        return False
