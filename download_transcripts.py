"""
Download a complete version of the transcripts from Australian Federal Parliament,
taking care to go slowly, and only retrieve things that have changed.

"""

# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "selenium",
# ]
# ///

import collections
import datetime
import os
import sqlite3
import tempfile
import time
import traceback
import xml.etree.ElementTree as ET

from urllib.parse import urlparse, parse_qs

from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By


def timestamp_now():
    return datetime.datetime.now(datetime.UTC).isoformat()


def extract_sitemap_components(sitemap_xml_str):
    """
    Extract the URL and lastmod dates for the sitemap.

    """
    tree = ET.fromstring(sitemap_xml_str)

    for url in tree.iter("{http://www.sitemaps.org/schemas/sitemap/0.9}url"):
        loc = url.find("{http://www.sitemaps.org/schemas/sitemap/0.9}loc").text
        lastmod = url.find("{http://www.sitemaps.org/schemas/sitemap/0.9}lastmod").text

        yield (loc, lastmod)


def init_and_refresh_sitemap(driver, db):
    """
    Initialise and update the local sitemap with a copy of the aph sitemap.

    Will interruptibly retrieve and refresh all sitemap parts until the sitemap is fully
    populated.

    """

    sitemap_entry_url = "https://parlinfo.aph.gov.au/sitemap/sitemapindex.xml"

    # Visit sitemap - we'll use Selenium DOM to work with the XML, rather than
    # trying to do it directly....
    driver.get(sitemap_entry_url)

    tree = ET.fromstring(driver.page_source)

    # Sitemaps that exist
    sitemaps = [
        loc.text
        for loc in tree.iter("{http://www.sitemaps.org/schemas/sitemap/0.9}loc")
    ]

    # Sitemaps that have already been retrieved - the init function just makes sure
    # we've visited everything at least once, the update function handles the logic
    # of ensuring we have an up to date snapshot.
    previously_retrieved = {row[0] for row in db.execute("""
            SELECT distinct source_sitemap
            from sitemap;
            """)}

    # These reference points are set on first init, and we retrieve sitemaps in
    # descending order of freshness of URLs - this gives a reliable point for
    # estimating the refresh point, and also allows us to estimate time since the last
    # full refresh.
    now = timestamp_now()
    db.execute(
        "INSERT or ignore into process_data values(?, ?)", ("last_refresh_time", now)
    )
    db.execute(
        "INSERT or ignore into process_data values(?, ?)",
        ("last_full_refresh_time", now),
    )

    # If the number of components changes from expected, halt early. We don't want to
    # sync an invalid sitemap and potentially discard valid work. This assumes that the
    # size of the sitemap is monotonically increasing.
    if len(sitemaps) < 2217:
        raise ValueError("Unexpectedly small sitemap components found - exiting")

    # Make sure we've retrieved all sitemap components.
    for i, source_sitemap in enumerate(reversed(sitemaps)):

        if source_sitemap in previously_retrieved:
            continue

        print(i + 1, "/", len(sitemaps), source_sitemap)

        db.execute("begin")

        driver.get(source_sitemap)

        for loc, lastmod in extract_sitemap_components(driver.page_source):
            db.execute(
                "REPLACE into sitemap values(?, ?, ?)", [loc, source_sitemap, lastmod]
            )
        db.execute("commit")

        time.sleep(15)

    print("Sitemap initialised.")

    last_refresh_time = list(
        db.execute("SELECT value from process_data where key = 'last_refresh_time'")
    )[0][0]

    # lastmod provided by the sitemap is only accurate up to the date - we'll use a
    # reference date of a few days before that to ensure that we have everything.
    refresh_delta = datetime.timedelta(days=3)
    reference_refresh = (
        datetime.datetime.fromisoformat(last_refresh_time) - refresh_delta
    )
    reference_date = reference_refresh.date().isoformat()

    print(f"Refreshing sitemap until {reference_date}.")

    # Now ensure we have an up to date copy of the sitemap, taking advantage of the fact
    # that the urls are provided in order of last modification date.
    for i, source_sitemap in enumerate(reversed(sitemaps)):
        db.execute("begin")

        driver.get(source_sitemap)

        max_lastmod = ""

        for loc, lastmod in extract_sitemap_components(driver.page_source):
            db.execute(
                "REPLACE into sitemap values(?, ?, ?)", [loc, source_sitemap, lastmod]
            )

            max_lastmod = max(lastmod, max_lastmod)

        db.execute("commit")

        if max_lastmod <= reference_date:
            break

        time.sleep(15)

    now = timestamp_now()
    db.execute("REPLACE into process_data values(?, ?)", ("last_refresh_time", now))


def identify_transcripts_to_retrieve(db):
    """
    Process the sitemap to identify Hansard transcripts that exist for retrieval.

    """

    db.execute("begin")
    possible_transcripts = db.execute("""
        SELECT url, lastmod
        from sitemap
        where instr(url, 'hansard')
        """)

    pages = collections.Counter()

    # Parse the URLs in the sitemap - to identify sitting days.
    # One daily transcript becomes multiple speeches bundled together with a pagination
    # number - we want to idenfity the first of these for each group and retrieve both
    # the HTML, and the XML or SGML transcripts for that page.
    for url, lastmod in possible_transcripts:

        # Parse the query to find the first page of each day - this is page 0000. Each
        # sitting day is split across multiple HTML pages, approximately one per
        # speech. Note that we're also filtering out some nonsensical URLs that appear
        # to correspond to data entry issues.
        url_params = urlparse(url).params
        query_id = parse_qs(url_params)["query"][0].split('"')[1]

        query_id_components = query_id.split("/")
        page_no = query_id_components[-1]
        pages[page_no] += 1

        if page_no == "0000":
            # This is an upsert - we mark it as an ancient retrieved date to always
            # mark new items as ready for retrieval.
            db.execute(
                "INSERT or ignore into hansard_transcript(url, retrieved) values(?, ?)",
                (url, "2000-01-01"),
            )
            db.execute(
                "UPDATE hansard_transcript set lastmod = ?2 where url = ?1",
                (url, lastmod),
            )

    print("Top 10 most common page numbers:", pages.most_common(10))

    db.execute("commit")


def retrieve_transcripts(driver, db, download_dir):
    """
    Retrieve outdated transcripts.

    This first retrieves the HTML page at the start of each sitting day, checks for the
    XML transcript link, and if not present it retrieves the inferred SGML link.

    The HTML page is retrieved to enable validation of the XML/SGML parsing for the
    necessary structural evaluation by examining the navigation elements to smaller
    units of text.

    """

    to_retrieve = list(r[0] for r in db.execute("""
            SELECT url 
            from hansard_transcript
            where retrieved < lastmod
            order by lastmod
            """))

    total_to_retrieve = len(to_retrieve)

    last_loop_start = 0
    failures = 0

    for i, url in enumerate(to_retrieve):
        print("Retrieving", i, "/", total_to_retrieve, url)

        # Handle failures by moving on - we'll try them again on the next run.
        try:
            now = timestamp_now()
            current_timestamp = time.monotonic()

            # target cycle time = 20 seconds. Max prevents sleeping for a negative
            # amount.
            sleep_time = 20 - (current_timestamp - last_loop_start)
            time.sleep(max(0, sleep_time))
            last_loop_start = time.monotonic()

            db.execute("begin")

            driver.get(url)

            html_page = driver.page_source

            links = [
                l.get_attribute("href") or ""
                for l in driver.find_elements(By.TAG_NAME, "a")
            ]

            transcript_link = None
            transcript_type = None
            pdf_link = None

            pdf_links = [l for l in links if "toc_pdf" in l]
            xml_links = [l for l in links if "toc_unixml" in l]

            assert len(pdf_links) <= 1
            # There are a few cases that don't have PDF transcripts that need
            # investigating
            pdf_link = None
            if pdf_links:
                pdf_link = pdf_links[0]

            assert len(xml_links) <= 1

            if xml_links:
                transcript_link = xml_links[0]
                transcript_type = "xml"

            # Generate the SGML link if no XML link is present. This is a bit of magic
            # from knowing the internals provided by Parl Library staff. We could just
            # use a fixed set of URLs for this component, but it seems better to be
            # aware of and respond to updates.
            if transcript_link is None:

                # Selenium only shows visible elements - there's a JS toggle that hides
                # most of the metadata by default
                check_input = driver.find_element(By.ID, "toggleMetadata")

                if check_input.get_attribute("alt") == "Expand":
                    check_input.click()

                keys = driver.find_elements(By.CLASS_NAME, "mdLabel")
                values = driver.find_elements(By.CLASS_NAME, "mdValue")

                mapping = {}

                for key, value in zip(keys, values):
                    if value.text.strip():
                        mapping[key.text.strip()] = value.text

                system_id_components = mapping["System Id"].split("/")
                transcript_id = "/".join(system_id_components[:-1])
                date = system_id_components[2]

                if system_id_components[1] == "hansardr":
                    house = "reps"
                elif system_id_components[1] == "hansards":
                    house = "senate"

                transcript_type = "sgml"

                transcript_url_file = f"{house}%20{date}.sgm"
                transcript_path = f"{house} {date}.sgm"
                transcript_link = (
                    "https://parlinfo.aph.gov.au/parlInfo/download/"
                    f"{transcript_id}/toc_sgml/{transcript_url_file}"
                )

            # There's some funky mix of content types for a few of the SGML transcripts,
            # so prepare to handle the edge cases. This would be simpler if we were
            # just using a library like requests, but then we'd need to juggle cookies
            # and JS for their WAF configuration.
            try:
                driver.get(transcript_link)
                transcript_markup = driver.page_source

            except TimeoutException:
                # Check the download temp directory and use that file if present,
                # otherwise continue without marking this as retrieved.

                if transcript_type == "sgml":
                    download_path = os.path.join(download_dir, transcript_path)

                    if os.path.exists(download_path):
                        with open(download_path, "r") as f:
                            transcript_markup = f.read()
                    else:
                        print(f"Failed to retrieve SGML transcript {transcript_link}")
                        raise
                else:
                    raise

            if transcript_markup.startswith("<html"):
                # Handle case for SGML with type application/plain get's wrapped in a
                # basic pre tag by firefox for display, which is the "page source"
                # according to the firefox/selenium combo.
                transcript_markup = None

                if transcript_type == "sgml":
                    pres = driver.find_elements(By.CSS_SELECTOR, "html > body > pre")

                    if len(pres) == 1:
                        transcript_markup = pres[0].text

                if transcript_markup is None:
                    print("Couldn't retrieve a transcript at", transcript_link)

            db.execute(
                """
                UPDATE hansard_transcript
                    set 
                        retrieved = :retrieved,
                        html_ref_page = :html,
                        transcript_pdf_url = :pdf_link,
                        transcript_markup_url = :transcript_url,
                        transcript_markup_type = :markup_type,
                        transcript_markup = :transcript_markup
                    where url = :url
                """,
                {
                    "retrieved": now,
                    "html": html_page,
                    "pdf_link": pdf_link,
                    "transcript_url": transcript_link,
                    "markup_type": transcript_type,
                    "transcript_markup": transcript_markup,
                    "url": url,
                },
            )

            db.execute("commit")

        except Exception as e:
            failures += 1
            print(traceback.format_exc())
            print(
                f"Uncaught exception for {transcript_link} with {url} - transcript not "
                "retrieved, continuing"
            )

            db.execute("rollback")

            # If multiple failures happen just exit - there might be a problem with the
            # retrieval method, or just an outage within parlinfo.
            if failures >= 10:
                break
            else:
                continue


if __name__ == "__main__":

    import os
    import sys

    args = sys.argv[1:]

    db = sqlite3.connect("transcripts_progress.db", isolation_level=None)

    db.executescript("""
        CREATE table if not exists sitemap(
            url primary key,
            source_sitemap,
            lastmod
        );

        CREATE table if not exists process_data(
            key text primary key,
            value
        );

        CREATE table if not exists hansard_transcript(
            url primary key,
            lastmod text,
            retrieved text,
            html_ref_page text,
            transcript_pdf_url text,
            transcript_markup_url text,
            transcript_markup_type text,
            transcript_markup text
        );

        pragma journal_mode=WAL;
        """)

    if "--skip-transcripts" not in args:
        with tempfile.TemporaryDirectory(dir=".") as tempdir:
            options = webdriver.FirefoxOptions()

            # The download options are necessary to handle the SGML files with mixed
            # application/content-type headers that lead to strange behaviour.
            options.set_preference("browser.download.dir", tempdir)
            options.set_preference("browser.download.folderList", 2)

            # Escape hatch via environment variables if geckodriver is installed
            # somewhere interesting, such as a snap on linux.
            if geckodriver_path := os.environ.get("WEBDRIVER_GECKO_DRIVER", None):
                service = webdriver.FirefoxService(geckodriver_path)
            else:
                service = webdriver.FirefoxService()

            driver = webdriver.Firefox(options=options, service=service)

            # This is a simple site - just rely on a basic page load timeout.
            driver.set_page_load_timeout(10)

            try:

                # Check and update transcripts
                init_and_refresh_sitemap(driver, db)
                identify_transcripts_to_retrieve(db)
                retrieve_transcripts(driver, db, tempdir)

            finally:
                driver.quit()

    if "--skip-handbook-data" not in args:
        db.execute("begin")
        # Update info from the parliamentary handbook
        # retrieve_ministers(db)
        # retrieve_electorates(db)
        retrieve_parliamentarians(db)
        retrieve_party_records(db)

        db.execute("commit")
