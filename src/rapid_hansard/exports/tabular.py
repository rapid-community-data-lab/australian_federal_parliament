"""
Functions for taking a prebuilt database of Federal parliamentary transcripts and
parliamentary handbook metdata and creating extracts in various tabular formats.

"""

from pathlib import Path
import datetime
import shutil
import sqlite3
import tempfile

import polars as pl


def export_parquet(parsed_db: str, output_folder: str) -> None:
    """
    Export the given database of prepared transcripts and handbook data into parquet files.

    This runs all the export helper functions for individual parquet tables.

    """

    output_folder = Path(output_folder)
    output_folder.mkdir(parents=True, exist_ok=True)

    db = sqlite3.connect(parsed_db, autocommit=True)

    print(f"Creating Parquet tables in {output_folder} from parsed database {parsed_db}")

    print("Creating session table.")
    parquet_session(db, output_folder / "session.parquet")

    print("Creating speaker table.")
    parquet_speaker(db, output_folder / "speaker.parquet")

    print("Creating paragraph (text) table.")
    parquet_paragraph(db, output_folder / "paragraph.parquet")


def parquet_session(db_connection: sqlite3.Connection, destination: Path) -> None:
    """Creates the table of legislative sessions (sittings)."""

    session_query = "SELECT * from session where date >= '1996-01-01'"

    sessions = pl.read_database(
        session_query,
        db_connection,
        schema_overrides={
            "session_id": pl.datatypes.Int64,
            "url": pl.datatypes.String,
            "transcript_pdf_url": pl.datatypes.String,
            # Load this as a string then convert to a native date - it's a bit tricky
            # to get the python/sqlite/pl round trip right so we'll just be explicit
            # here.
            "date": pl.datatypes.String,
            "chamber": pl.datatypes.String,
        },
    )

    with tempfile.TemporaryDirectory() as tempdir:

        working_file = Path(tempdir, "session.parquet")

        fixed_date = sessions.with_columns(date=pl.col("date").str.to_date("%Y-%m-%d"))
        fixed_date.write_parquet(working_file, compression="zstd", compression_level=22)

        shutil.move(working_file, destination)


def parquet_speaker(db_connection: sqlite3.Connection, destination: Path) -> None:
    """
    Speaker table, mostly using information derived from the parliamentary handbook.

    Currently this only includes recognised parliamentarians: guest speakers are not
    included at all despite being present in the record.

    """

    speaker_query = """
        SELECT
            phid,
            display_name,
            gender
            -- TODO: link to the parliamentary handbook for this person.
            -- TODO: fix the source handling so absent dates are null, handle year
            -- only birthdates appropriately.
            -- date_of_birth,
            -- date_of_death
        from parliamentarian
        """

    speakers = pl.read_database(
        speaker_query,
        db_connection,
        schema_overrides={
            "phid": pl.datatypes.String,
            "display_name": pl.datatypes.String,
            "gender": pl.datatypes.String,
            # See date notes in parquet_session
            # "date_of_birth": pl.datatypes.String,
            # TODO: fix null handling here
            # "date_of_death": pl.datatypes.String,
        },
    )

    with tempfile.TemporaryDirectory() as tempdir:

        working_file = Path(tempdir, "speaker.parquet")

        # TODO when adding dates back.
        # fixed_date = speakers.with_columns(
        #   date_of_birth=pl.col("date_of_birth").str.to_date("%Y-%m-%d"),
        #   date_of_death=pl.col("date_of_death").str.to_date("%Y-%m-%d"))

        speakers.write_parquet(working_file, compression="zstd", compression_level=22)

        shutil.move(working_file, destination)


def parquet_paragraph(db_connection: sqlite3.Connection, destination: Path) -> None:
    """
    The paragraph tables contains one row per marked up paragraph in the source docs.

    """

    paragraph_query = """
        SELECT
            para_id,
            session_id,
            sequence_number,
            speaker_id,
            debate_id,
            fragment_number as procedural_unit_number,
            fragment_type as procedural_unit_type,
            paragraph_text as text
        from paragraph
        where session_id in (select session_id from session where date >= '1996-01-01')
        """

    # This is the biggest table so we need to work in batches instead.
    paragraph_batches = pl.read_database(
        paragraph_query,
        db_connection,
        iter_batches=True,
        batch_size=1000000,
        schema_overrides={
            "para_id": pl.datatypes.Int64,
            "session_id": pl.datatypes.Int64,
            "sequence_number": pl.datatypes.Int64,
            "speaker_id": pl.datatypes.String,
            "debate_id": pl.datatypes.Int64,
            "procedural_unit_number": pl.datatypes.Int64,
            "procedural_unit_type": pl.datatypes.String,
            "text": pl.datatypes.String,
        },
    )

    with tempfile.TemporaryDirectory() as tempdir:

        # Write batches out one a time.
        for i, df in enumerate(paragraph_batches):
            print(f"Preparing batch {i+1}")
            working_file = Path(tempdir, f"paragraph{i}.parquet")
            df.write_parquet(working_file)

        # Put the batches together into one convenient file
        batches = pl.scan_parquet(
            Path(tempdir, "paragraph*.parquet")
        )
        # TODO: there's probably a benefit to ordering this somehow...
        combined_file = Path(tempdir, f"paragraph.parquet")
        batches.sink_parquet(combined_file, compression="zstd", compression_level=22)

        shutil.move(combined_file, destination)