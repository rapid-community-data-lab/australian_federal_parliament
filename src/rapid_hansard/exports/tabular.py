"""
Functions for taking a prebuilt database of Federal parliamentary transcripts and
parliamentary handbook metdata and creating extracts in various tabular formats.

"""

from pathlib import Path
import datetime
import shutil
import sqlite3
import tempfile

import polars


def export_parquet(parsed_db: str, output_folder: str):
	"""Export the given database of prepared transcripts and handbook data into parquet files."""
	
	output_folder = Path(output_folder)
	output_folder.mkdir(parents=True, exist_ok=True)	

	db = sqlite3.connect(parsed_db, autocommit=True)

	print(f"Creating Parquet tables in {output_folder} from parsed database {parsed_db}")

	print("Creating session table.")

	session_query = "SELECT * from session where date >= '1996-01-01'"

	# We do this incrementally in batches to avoid using heaps of memory
	sessions = polars.read_database(
	    session_query,
	    db,
	    schema_overrides={
	        "session_id": polars.datatypes.Int64,
	        "url": polars.datatypes.String,
	        "transcript_pdf_url": polars.datatypes.String,
	        # I think we need to override this in a bit to get a proper round trip
	        # from SQLite?
	        "date": polars.datatypes.String,
	        "chamber": polars.datatypes.String,
	    },
	)

	with tempfile.TemporaryDirectory() as tempdir:

		working_file = Path(tempdir, "session.parquet")

		sessions.write_parquet(working_file, compression="zstd", compression_level=22)

		shutil.move(working_file, output_folder / "session.parquet")
