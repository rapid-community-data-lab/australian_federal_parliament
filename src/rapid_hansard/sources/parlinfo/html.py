import datetime as dt
from lxml.html import document_fromstring

from html import unescape
import logging


logger = logging.getLogger(__name__)


def get_metadata_from_html(html_url, html_text) -> dict | None:
    try:
        doc = document_fromstring(html_text)
    except Exception as e:
        logger.error(f"Could not parse HTML source downloaded from {html_url}, skipping")
        logger.exception(e)
        return None

    doc.make_links_absolute("https://parlinfo.aph.gov.au")

    try:
        metadata_section = doc.find_class('metadata')[0]
    except IndexError:
        logger.info(f"HTML page downloaded from {html_url} doesn't have a metadata section")
        return {}

    # Extract the metadata tags from the relevant section by
    # reassembling the definition list.
    dts = metadata_section.xpath(".//dt")
    dds = metadata_section.xpath(".//dd")

    key_values = (
        # Sometimes the empty values are filled with the HTML escape
        # &nbsp, sometimes they're filled with the '\xa0' character...
        (key.text, unescape("".join(value.itertext())).strip())
        for key, value in zip(dts, dds)
    )

    metadata = {key: value for key, value in key_values if value}

    try:
        transcript_date = dt.datetime.strptime(metadata['Date'], "%d-%m-%Y").date()
        metadata['session_date'] = transcript_date
    except KeyError:
        pass
    except ValueError:
        logger.exception(
            f"Could not parse 'Date' string {metadata['Date']} in metadata for transcript downloaded from{html_url}"
        )

    metadata['session_room'] = metadata.get('Source', None) or metadata.get('Database', None) or None

    if metadata['session_room'] is None:
        logger.debug('metadata room problem for url ' + html_url)

    return metadata



