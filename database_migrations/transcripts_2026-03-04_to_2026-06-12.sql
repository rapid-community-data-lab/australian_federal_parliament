alter table hansard_transcript
add column session_date text;

alter table hansard_transcript
add column session_room text;

update rapid_meta
set value = "2026-06-12"
where key = "transcript_db_version";