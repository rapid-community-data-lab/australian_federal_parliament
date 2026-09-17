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

    print("Creating speaker details table.")
    parquet_speaker_details(db, output_folder / "speaker_details.parquet")

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


def parquet_speaker_details(db_connection: sqlite3.Connection, destination: Path) -> None:
    """
    Speaker information, as of the date they were speaking.

    This allows accounting for changing details of speakers over time, such as which
    party they were in or which electorate they represented.

    """

    db_connection.execute(
        """
        CREATE temporary table speaker_asof (
            speaker_detail_id integer primary key,
            phid text,
            given_name text,
            family_name text,
            gender text,
            party text,
            valid_from date,
            valid_to date,
            unique(phid, valid_from, valid_to)
        )
        """
    )

    generate_speakers = db_connection.execute(
        """
        REPLACE into speaker_asof
            WITH active_speaker as (
                SELECT speaker_id, date
                from paragraph
                inner join session using(session_id)
                where date >= '1996-01-01'
            )
            SELECT
                null,
                parliamentarian.phid,
                given_name,
                family_name,
                gender,
                party.name,
                start_date,
                coalesce(end_date, '3000-01-01')
                -- TODO: link to the parliamentary handbook for this person.
                -- TODO: fix the source handling so absent dates are null, handle year
                -- only birthdates appropriately.
                -- date_of_birth,
                -- date_of_death
            from parliamentarian
            inner join active_speaker on parliamentarian.phid = active_speaker.speaker_id
            inner join party_member on
                party_member.phid = parliamentarian.phid and
                date >= start_date and
                date < coalesce(end_date, '3000-01-01')
            inner join party using (party_id)
        """
    )

    speaker_details = pl.read_database(
        "SELECT * from speaker_asof",
        db_connection,
        schema_overrides={
            'valid_to': pl.datatypes.String, 'valid_from': pl.datatypes.String
        }
    )

    with tempfile.TemporaryDirectory() as tempdir:

        working_file = Path(tempdir, "speaker_details.parquet")

        fixed_date = speaker_details.with_columns(
            valid_from=pl.col("valid_from").str.to_date("%Y-%m-%d"),
            valid_to=pl.col("valid_to").str.to_date("%Y-%m-%d")
        )
        speaker_details.write_parquet(working_file, compression="zstd", compression_level=22)

        shutil.move(working_file, destination)


def parquet_paragraph(db_connection: sqlite3.Connection, destination: Path) -> None:
    """
    The paragraph tables contains one row per marked up paragraph in the source docs.

    This requires that parquet_speaker_details has been run first to generate the
    appropriate speaker_asof table to join against.

    """

    paragraph_query = """
        SELECT
            para_id,
            session_id,
            sequence_number,
            speaker_id,
            speaker_detail_id,
            debate_id,
            fragment_number as procedural_unit_number,
            fragment_type as procedural_unit_type,
            paragraph_text as text
        from paragraph
        inner join session using(session_id)
        left outer join speaker_asof on
            speaker_id = phid and
            date >= valid_from and
            date < coalesce(valid_to, '3000-01-01')
        where date >= '1996-01-01'
        """

    # This is the biggest table so we need to work in batches instead.
    paragraph_batches = pl.read_database(
        paragraph_query,
        db_connection,
        iter_batches=True,
        batch_size=500000,
        schema_overrides={
            "para_id": pl.datatypes.Int64,
            "session_id": pl.datatypes.Int64,
            "sequence_number": pl.datatypes.Int64,
            "speaker_id": pl.datatypes.String,
            "speaker_detail_id": pl.datatypes.Int64,
            "debate_id": pl.datatypes.Int64,
            "procedural_unit_number": pl.datatypes.Int64,
            "procedural_unit_type": pl.datatypes.String,
            "text": pl.datatypes.String,
        },
    )

    with tempfile.TemporaryDirectory() as tempdir:

        # Write batches out one a time.
        for i, df in enumerate(paragraph_batches):
            working_file = Path(tempdir, f"paragraph{i:03}.parquet")
            df.write_parquet(working_file)

        # Put the batches together into one convenient file
        batches = pl.scan_parquet(
            Path(tempdir, "paragraph*.parquet")
        )
        # TODO: there's probably a benefit to ordering this somehow...
        combined_file = Path(tempdir, f"paragraph.parquet")
        batches.sink_parquet(combined_file, compression="zstd", compression_level=22)

        shutil.move(combined_file, destination)