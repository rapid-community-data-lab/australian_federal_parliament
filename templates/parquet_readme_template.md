# Proceedings of Australian Federal Parliament in Analysis Ready Format

This is a collection of four parquet files created from the official transcripts of the Proceedings of Australian Federal Parliament. Each file contains one table of information relating to the proceedings, including the text of what was recorded in the proceedings, information about the speaker at the time they were speaking (where available), information about the procedural context in which they were speaking, and when and where they were speaking.

These files are published in parquet format because it enables high performance computational analytics, high compression (ie. faster download time), and is usable with a number of common data analysis toolkits and software libaries.


## File Overview

The details of each file are shown below. Note that this will change and evolve over time as we standardise and improve how we handle the transcripts and in response to researchers' needs.


### Session

(Note this will be renamed in future versions to avoid confusion between a sitting day and a legislative session within a government.)

A session is a single sitting on a single day of a particular chamber of Federal Parliament. The date is nominal and records the starting day of the sitting: sittings can and do stretch into the next day.

- *session_id*: An internally consistent identifier for this sitting of a chamber.
- *url*: The link to the start of this sittings transcript in ParlInfo.
- *transcript_pdf_url*: The sittings transcript in PDF format.
- *date*: The date of the sitting.
- *chamber*: The chamber of parliament meeting in this sitting ('House' or 'Senate')


### Speaker Detail

The speaker detail table records information about speakers *as of the time they are speaking*. Many details about speakers change over time including their electorates, party memberships or ministerial roles. This table condenses that to a unique `speaker_detail_id` that represents a unique combination of attributes that are valid for a specified time period.

This can be filtered directly to find when particular speakers had various roles, or joined directly with the `paragraph` table to find text by people having particular characteristics that may change over time. Examples include: identifying when the then Minister for Education mentions 'local schools'.

- *speaker_detail_id*: An identifier for this particular speaker's details for the time range identifier in valid_from/valid_to.
- *phid*: The Parliamentary Libraries identifier for this parliamentarian.
- *given_name*: The Parliamentarian's given name in the Parliamentary Handbook.
- *family_name*: The Parliamentarian's family name in the Parliamentary Handbook.
- *gender*: The Parliamentarian's gender as in the Parliamentary Handbook.
- *party*: The Parliamentarian's party membership in the Parliamentary Handbook.
- *valid_from*: The starting date of validity of this row.
- *valid_to*: The (exclusive) ending date of validity of this row.


### Debate Title

Debate title captures the procedural order of business and the associated hierarchical structure assigned by the transcription process. Titles are rarely explicitly stated in the audio of proceedings, but do tend to follow the order of the day's business as decided by that chamber.

- *debate_id*: The unique identifier for this debate title in the transcript. Consecutive sequences with the same debate title in the same sitting transcript are assigned the same ID. But debates can be adjourned and resumed on the same or different days, so this is not a reliable structure for counting.
- *session_id*: The identifier for the sitting day of this debate.
- *debate_no*: The sequential number of the debate on the day, taking into account that this may include non-real changes that are a productive of how transcripts are encoded.
- *title*: The title of this particular debate. This is formed by concatenating all title lines for all enclosing headings for the debate at this point.


### Paragraph

The paragraph table holds the non-heading text of the transcript and corresponds approximately to what is said (or entered into on behalf of) for the Hansard record. This includes: speeches, questions and answers (both with and without notice so may be spoken or written), presentation of petitions, motions, the text included within tables and more.

- *para_id*: A unique identifier for this paragraph.
- *session_id*: The session for this paragraph of text.
- *sequence_number*: The sequence number of this paragraph within the days transcript.
- *speaker_id*: The transcripts speaker identifier, which is typically a parliament house assigned ID, but may also include various role and other IDs (for example, the speaker has a separate identifier to their parliamentary identifier)
- *speaker_detail_id*: A key in the speaker_detail table that gives the details of this speaker _at the point in time of this sitting day_
- *debate_id*: A reference to the debate title table for retrieving the procedural headings/context.
- *procedural_unit_number*: An identifier for the procedural unit 
- *procedural_unit_type*: The procedural unit in which this paragraph is embedded (typically a speech, but it could also be a motion, a petition, a question or answer).
- *text*: The text recorded in Hansard that is directly attributable to the speaker. Elements such as speaker names, ministerial roles, timestamps, electorates that are procedurally generated and inserted for reading context are removed.


## How this was prepared

This dataset was prepared by the RAPID-CDL project from the official parliamentary proceedings published by the Australian Federal Parliamentary Library.

The source code is available under an open licence: https://github.com/rapid-community-data-lab/australian_federal_parliament (archived at https://doi.org/10.5281/zenodo.18946434). 


## License and Acknowledgement

The source transcripts are published under the [CC-BY-NC-ND licence by the Parliamentary Library of Australian Federal Parliament.](https://www.aph.gov.au/Help/Disclaimer_Privacy_Copyright#c)