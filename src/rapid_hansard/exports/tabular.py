"""
Functions for taking a prebuilt database of Federal parliamentary transcripts and
parliamentary handbook metdata and creating extracts in various tabular formats.

"""

from pathlib import Path
import datetime
import shutil
import sqlite3

import polars

# Note - isolation_level=None won't work in future versions of Python (sometime after
# 3.13), this should be using the autocommit=True value instead, but requires some
# investigation for different python versions
# try:
# 	db = sqlite3.connect("../../oz_federal_hansard.db", autocommit=True)
# except TypeError:

# db.executescript("""
#     CREATE temporary table contains_interjection (
#         para_id integer primary key,
#         interjection bool
#     );

#     insert into contains_interjection
#         select distinct
#             para_id, 
#             1
#         from paragraph_enclosed_class
#         where lower(class) like '%inter%';
#     """)

# query = """
# SELECT
#     session.date,
#     session.chamber,
#     session.transcript_pdf_url,
#     debate.title as debate_title,
#     fragment_number as speech_number,
#     case when interjection then 'interjector' else parliamentarian.display_name end
#         as speaker,
#     case when interjection then null else party.name end as party,
#     case when interjection then null else parliamentarian.gender end
#         as gender,
#     case when interjection then null else 
#         -- time diff as an iso8601 like string, then just pulling the years out.
#         cast(substr(timediff(session.date, parliamentarian.date_of_birth), 4, 2) as integer) end
#         as age,
#     paragraph.paragraph_text || '\n' as paragraph_text
# -- from (select * from paragraph limit 1000) paragraph
# from paragraph
# inner join session using(session_id)
# inner join debate using(debate_id)
# left outer join parliamentarian on speaker_id = parliamentarian.phid
# left outer join party_member on speaker_id = party_member.phid
#     and session.date between party_member.start_date and 
#         coalesce(party_member.end_date, '3000-01-01')
# left outer join party using(party_id)
# left outer join contains_interjection using(para_id)

# where session.date >= '1996-01-01'

# order by session.date, chamber, speech_number

# """

# output = "proceedings_of_federal_parliament_2010_2026.parquet"
# print(f"exporting to: {output}")

# # We do this incrementally in batches to avoid using heaps of memory
# rows = polars.read_database(
#     query,
#     db,
#     iter_batches=True,
#     batch_size=100000,
#     schema_overrides={
#         "date": polars.datatypes.String,
#         "chamber": polars.datatypes.String,
#         "transcript_pdf_url": polars.datatypes.String,
#         "debate_title": polars.datatypes.String,
#         "speech_number": polars.datatypes.Int64,
#         "speaker": polars.datatypes.String,
#         "party": polars.datatypes.String,
#         "gender": polars.datatypes.String,
#         "age": polars.datatypes.Int64,
#         "paragraph_text": polars.datatypes.String,
#     },
# )

# temp_data = pathlib.Path(".temp_data")
# try:
#     temp_data.mkdir(exist_ok=True)

#     for i, df in enumerate(rows):
#         print(f"processing batch {i+1}")
#         transformed = df.with_columns(date=polars.col("date").str.to_date("%Y-%m-%d"))
#         transformed.write_parquet(
#             temp_data / f"proceedings_of_federal_parliament_{i}.parquet"
#         )

#     # Put the batches together into one convenient file
#     source = polars.scan_parquet(
#         temp_data / "proceedings_of_federal_parliament_*.parquet"
#     )
#     source.sink_parquet(output, compression_level=22, compression="zstd")

# finally:
#     # Cleanup
#     shutil.rmtree(temp_data, ignore_errors=True)


def export_parquet(parsed_db: str, output_folder: str):
	"""Export the given database of prepared transcripts and handbook data into parquet files."""
	
	Path(output_folder).mkdir(parents=True, exist_ok=True)	

	db = sqlite3.connect(parsed_db, autocommit=True)

	print(list(db.execute("select count(*) from paragraph")))

	return