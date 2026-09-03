import click
from pathlib import Path
import logging
import sqlite3

from rapid_hansard.__init__ import __version__
from rapid_hansard.sources.parlinfo import run_transcript_download, initialise_db, process_transcripts
from rapid_hansard.sources.parliamentary_handbook import fetch_all as ph_fetch_all
from rapid_hansard.exports.tabular import export_parquet


logger = logging.getLogger(__name__)


help_examples = """
Basic usage (in sequence):\n
  rapid_hansard fetch transcripts transcripts.db\n
  rapid_hansard parse transcripts.db rapid_hansard.db\n
  rapid_hansard fetch parliamentary_handbook rapid_hansard.db\n
  rapid_hansard export parquet rapid_hansard.db export_parquet/
  ... (exporting tbd)\n
See https://rapid-cdl.edu.au for more information about the RAPID-CDL project and this software.
"""

@click.group(epilog=(help_examples))
@click.version_option(prog_name="RAPID_Hansard", package_name="rapid_hansard", version=__version__)
@click.option("--debug", is_flag=True, help="Turn on debug logging")
def cli(debug):
    """A utility to enable fetching (downloading), parsing, and exporting Australian Hansard transcript data."""
    log_level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(filename='rapid_hansard.log', encoding='utf-8', level=log_level)


@cli.group(epilog=("Examples:\n\n\b\n"
                   "  rapid-hansard fetch transcripts transcript_files.db\n\n\b\n"
                   "  rapid-hansard fetch parliament_data\n"))
def fetch():
    """
    Commands to download data from the Australian Parliament House website. Data is stored in local database files,
    ready for processing with `rapid-hansard parse`
    """
    pass


@fetch.command(short_help="download Hansard transcripts")
@click.argument('database', type=click.Path(),
                required=False, default="transcripts.db")
@click.option('--full-refresh-sitemap', 'full_refresh_sitemap', is_flag=True)
def transcripts(database: click.Path, full_refresh_sitemap):
    """
    Downloads Hansard transcripts from the Australian Parliament House website. If an existing transcript database is
    provided, this will update and add new transcripts to that database rather than starting the download from scratch.
    If a filename (e.g. transcripts.db) is provided for a database that doesn't already exist, a new one will be created.

    Example: rapid_hansard fetch transcripts transcripts.db

    Note this relies on firefox and geckodriver installed and findable by selenium: if you
    have firefox installed via snap you will need to use the WEBDRIVER_GECKO_DRIVER
    environment variable to point it at the right location.

    WEBDRIVER_GECKO_DRIVER=/snap/bin/geckodriver rapid_hansard fetch transcripts transcripts.db
    """
    database = Path(database)

    if database.exists:
        message = f"Using existing database {click.format_filename(database)}"
    else:
        message = f"Creating new database {click.format_filename(database)}"
    click.echo(message)
    logger.info(message)

    db_connection = sqlite3.connect(database, isolation_level=None)

    initialise_db(db_connection, full_refresh_sitemap)

    run_transcript_download(db_connection)


@fetch.command()
@click.argument('database', type=click.Path(), default="rapid_hansard.db")
def parliament_data(database: Path):
    """
    Fetches data about parliamentarians, parties, and other parliamentary entities from the
    Parliament. To be used with the processed/parsed database, not the transcript database.
    """
    ph_fetch_all(database)


@cli.command()
@click.argument('transcript_db', type=click.Path(exists=True), default="transcripts.db")
@click.argument('parsed_db', type=click.Path(), default="rapid_hansard.db")
@click.option('--skip_format', default=None, type=click.Choice(['xml', 'sgml'], case_sensitive=False),
              help="Use this option to skip processing transcripts in a given format (SGML or XML).")
def parse(transcript_db: Path, parsed_db: Path, skip_format: str|None):
    """
    Takes a database of downloaded transcripts, and parses those transcripts into the RAPID-CDL Hansard data model. A
    new database will be created (any existing database at the `parsed_db` file path will be overwritten) for the
    processed transcript data.

    Arguments:\n
        transcript_db: The database that the transcripts have been downloaded into. e.g. transcripts.db\n
        parsed_db: Filename for the database of parsed transcript data. If the database already exists, it will be overwritten. Example: rapid_hansard.db
    """
    process_transcripts(transcript_db, parsed_db, skip_format)


@cli.group(epilog=("Examples:\n\n\b\n"
                   "  rapid-hansard export parquet rapid_hansard.db --output_folder=rapid_parquet\n\n\b\n"))
def export():
    """
    Commands to export data that has been parsed using `rapid-hansard parse` into a variety of formats.

    """
    pass


@export.command()
@click.argument('parsed_db', type=click.Path(exists=True), default="rapid_hansard.db")
@click.argument('output_folder', type=click.Path(file_okay=False, dir_okay=True, writable=True), default=Path("exports")/"parquet")
def parquet(parsed_db: str, output_folder: str):
    """
    Takes a database of prepared transcripts, and creates a set of parquet files representing the main tables of interest.

    Arguments:\n
        parsed_db: Filename for the database of parsed transcript data. Example: rapid_hansard.db
        output_folder: Output folder to hold parquet tables. The folder will be created if it
            doesn't already exist. Existing files will be overwritten.

    """
    export_parquet(parsed_db, output_folder)


@cli.command()
def inspect():
    """Not yet implemented"""
    pass


if __name__ == "__main__":
    cli()
