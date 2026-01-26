"""
Construct an index of tags and which transcripts they occur in.

This index is used to support investigating and understanding how the markup is used
across the entire set of transcripts.

This script also processes the HTML from parlinfo to count the number of navigation
fragments for comparison with other processing.

"""

# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "lxml"
# ]
# ///

import collections
import concurrent.futures as cf
import sqlite3

from html.parser import HTMLParser

import lxml.html


class StartEndTagParser(HTMLParser):
    """
    Investigate validity of tags across all the transcripts.

    This is to:

    1. Infer the structure of the SGML and which tags have implied optional start/ends
    2. Confirm the basic structural validity (or not) of the XML.

    """

    def __init__(self):
        super().__init__()
        self.start_tag_counts = collections.Counter()
        self.end_tag_counts = collections.Counter()

    @property
    def tag_total_counts(self):

        keys = set(self.start_tag_counts) | set(self.end_tag_counts)

        return {
            key: (self.start_tag_counts[key], self.end_tag_counts[key]) for key in keys
        }

    def handle_starttag(self, tag, attrs):
        self.start_tag_counts[tag] += 1

    def handle_endtag(self, tag):
        self.end_tag_counts[tag] += 1


def count_tags(transcript_key, transcript_type, transcript_str):
    """
    Extract the set of tag paths existing in the transcript.

    """

    parser = StartEndTagParser()

    parser.feed(transcript_str)

    all_tags = parser.tag_total_counts

    return (
        transcript_key,
        transcript_type,
        all_tags,
    )


def insert_tag_counts(db, extracted_tag_counts):
    """Insert extracted tag counts into the DB."""

    for transcript_key, transcript_type, tags in extracted_tag_counts:
        db.executemany(
            """
            INSERT into transcript_tag values (?, ?, ?, ?, ?)
            """,
            (
                (transcript_key, transcript_type, tag, start_count, end_count)
                for tag, (start_count, end_count) in tags.items()
            ),
        )


def count_parlinfo_nav_items(transcript_key, html_str):
    """
    Count the table of contents items for each day's transcript from the HTML.

    This is used to cross-check that the processing of the SGML/XML transcripts has
    the correct number of structural entities for interpreting the parliamentary
    context.

    """

    parsed = lxml.html.fromstring(html_str)

    nav_hierarchy_items = parsed.xpath("//ul[@id='tocMenu']//li")
    nav_links = parsed.xpath("//ul[@id='tocMenu']//a")

    return transcript_key, len(nav_hierarchy_items), len(nav_links)


if __name__ == "__main__":

    transcript_db = sqlite3.connect("transcripts_progress.db", isolation_level=None)
    index_db = sqlite3.connect("transcript_markup_index.db", isolation_level=None)

    index_db.executescript(
        """
        DROP table if exists transcript_tag;
        DROP table if exists transcript_toc;

        CREATE table transcript_tag(
            url,
            transcript_type,
            tag,
            start_count,
            end_count,
            primary key (url, tag)
        );

        CREATE table transcript_toc(
            url primary key,
            toc_item_count,
            toc_link_count
        );


        pragma journal_mode=WAL;
        """
    )

    with cf.ProcessPoolExecutor(8) as pool:

        index_db.execute("begin")

        # Process HTML to get the table of contents number of items for each transcript
        html_tables_of_contents = transcript_db.execute(
            """
            SELECT url, html_ref_page
            from hansard_transcript
            where retrieved is not null
                and transcript_markup is not null
            order by url
            """
        )

        tasks_in_flight = set()
        toc_processed = 0

        # Unlike the transcript processing this is fast, so doesn't really need a
        # process pool, but since we have it set up anyway...
        for url, html_toc in html_tables_of_contents:

            tasks_in_flight.add(pool.submit(count_parlinfo_nav_items, url, html_toc))

            if len(tasks_in_flight) > 200:
                completed, tasks_in_flight = cf.wait(tasks_in_flight, timeout=1)

                index_db.executemany(
                    "INSERT into transcript_toc values(?, ?, ?)",
                    (f.result() for f in completed),
                )

                toc_processed += len(completed)
                print("Completed:", toc_processed)

        # Don't forget to process the final batch
        index_db.executemany(
            "INSERT into transcript_toc values(?, ?, ?)",
            (f.result() for f in tasks_in_flight),
        )

        ## Process the tag counts for each transcript
        transcripts = transcript_db.execute(
            """
            SELECT url, transcript_markup_type, transcript_markup
            from hansard_transcript
            where retrieved is not null
                and transcript_markup is not null
            order by url
            """
        )

        tasks_in_flight = set()
        transcripts_processed = 0

        # Note that this is done using a process pool because this is otherwise quite
        # slow.
        for url, transcript_type, transcript in transcripts:

            tasks_in_flight.add(
                pool.submit(count_tags, url, transcript_type, transcript)
            )

            if len(tasks_in_flight) > 200:
                completed, tasks_in_flight = cf.wait(tasks_in_flight, timeout=1)

                insert_tag_counts(index_db, [f.result() for f in completed])

                transcripts_processed += len(completed)
                print("Completed:", transcripts_processed)

        # Don't forget to process the final batch
        insert_tag_counts(index_db, [f.result() for f in tasks_in_flight])

        index_db.execute("commit")
