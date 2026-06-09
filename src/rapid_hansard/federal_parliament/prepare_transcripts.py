"""
Process the transcripts into a more tractable computational form.

This process aims to identify text at the most granular transcribed structural unit, the
paragraph, and match it to the procedural context and who is speaking. The transcripts
as transcribed represent the procedural notion of how parliament operates which makes
certain kinds of information difficult to extract.

"""

# /// script
# requires-python = ">=3.12"
# ///

import collections
import concurrent.futures as cf
import dataclasses as dc
import html
import itertools
import re
import sqlite3
import xml.etree.ElementTree as ET
import logging

from rapid_hansard import __version__ as rapid_hansard_version

rapid_hansard_schema_version = "2026-03-04"


logger = logging.getLogger(__name__)


@dc.dataclass
class TranscriptContext:
    """Keeps track of all enclosing context for an element."""

    debate_info: tuple = dc.field(default_factory=tuple)
    enclosing_tags: set = dc.field(default_factory=set)
    enclosed_tags: set = dc.field(default_factory=set)
    # This is needed for the newest transcripts, as some of the info is encoded here
    # implicitly.
    enclosed_classes: set = dc.field(default_factory=set)
    speaker: dict = dc.field(default_factory=dict)
    fragment_number: int = 0
    fragment_type: str = None


def process_debate_info(element):
    """
    Extract information about the current state of the debate from the given element.

    Returns an empty list if this element has no information about the debate.

    """

    info = []

    if element.tag in ("debate"):
        # Note slightly awkward - the standard library etree doesn't support full xpath
        for debateinfo in itertools.chain(
            # debateinfo is standard, there's a couple of instances of debate.info
            element.findall("debateinfo"),
            element.findall("debate.info"),
        ):
            info.append({elem.tag: elem.text for elem in debateinfo})

    elif element.tag in (
        # Standard forms:
        "subdebate.1",
        "subdebate.2",
        "subdebate.3",
        "subdebate.4",
        # Mispellings/potential misuse
        "subdeabte.1",
        "subdebate",
    ):

        for debateinfo in itertools.chain(
            # Standard
            element.findall("subdebateinfo"),
            # Couple of elements only
            element.findall("subdebateinfo.1"),
        ):
            info.append({elem.tag: elem.text for elem in debateinfo})

    return info


def remove_para_markup(paragraph):
    """
    Extract plain text of paragraph, and normalise whitespace/newlines.

    """

    extracted_text = "".join(paragraph.itertext())

    return " ".join(extracted_text.split())


def process_xml_transcript(transcript_key, transcript_pdf_url, xml_str):
    """
    Extract text units from XML transcripts, with sufficient information about context.
    """

    try:
        root = ET.fromstring(xml_str)
    except ET.ParseError as e:
        logger.exception(
            f"Parsing (XML) error occurred for transcript {transcript_key}"
        )
        raise e

    # Session information first - this is the basic information about the date, house,
    # etc. and is the same across all of the elements in this transcript.
    session = root.find("session.header")
    session_info = {elem.tag: elem.text for elem in session}

    # The context holds information from the parent elements - it may be added or
    # extended as processing continues.
    context = TranscriptContext()

    # Note we're using a while loop and a queue to avoid Python limitations with
    # recursion. We still process in depth first order though so we can assign sensible
    # sequence numbers to the output processed items.

    to_process = [(context, root)]
    processed = []
    speaker = {}

    fragment_number = 0
    fragment_type = None

    while to_process:

        context, element = to_process.pop()

        tag = element.tag

        # There's mostly no title associated with these, but they are still distinct
        # procedural units. Notes:
        #   - Questions and answers are usually part of the same fragment
        #   - petitions rarely have embedded speeches - in some cases this will generate
        #     a fragment_number that isn't used.
        if tag in ("speech", "motionnospeech", "petition", "question", "answer"):
            fragment_number += 1
            fragment_type = tag

        # Extract debate info if present.
        new_debate_info = process_debate_info(element)

        if new_debate_info:
            last_speaker = {}

            # Reset speakers when new debate context is started.
            context = dc.replace(
                context,
                debate_info=tuple(
                    itertools.chain(context.debate_info, new_debate_info)
                ),
                speaker={},
            )

        # Speaker information varies quite a bit.
        # In newer transcripts, the talker tag appears only at the start of the speech
        # for the person who has the procedural floor - the actual speaker and changes
        # in speakers are marked in the individual p tags inside the talk.text entry and
        # interjections/continuations are only
        elif tag == "talker":
            speaker = {elem.tag: elem.text for elem in element}
            context = dc.replace(context, speaker=speaker)

        # Skip these (for now) - they occur only in the newest transcript format and are
        # equivalent to the debate_info we've already collected. The only reason to
        # look deeper into this is that for some areas such as bills, the reference IDs
        # of the bills are (now) included in this section but not the debate info.
        elif tag in ("debate.text", "subdebate.text"):
            continue

        # Finally - the thing we actually care about - the paragraphs of text
        # TODO: handle context from the p elements in the newer style transcripts.
        elif tag in ("p", "para"):

            # TODO: handle procedural stuff, like speaker names embedded in the text.
            paragraph_text = remove_para_markup(element)

            enclosed_tags = set()
            enclosed_classes = set()

            for e in element.iter():
                enclosed_tags.add(e.tag)
                if "class" in e.attrib:
                    enclosed_classes.add(e.attrib["class"])

            # For new style paragraph tags, look for the speaker ID in the href.
            # TODO: for p tags, the important info is contained in the classes applied
            # to different sections, not enclosing information like 'quote' tags etc.
            if tag == "p":

                anchor = element.find("a")

                if anchor is not None:
                    if "href" in anchor.attrib:
                        speaker = {}
                        speaker["name.id"] = anchor.attrib["href"]

            # Always attach the current speaker reference - this means that runs of
            # paragraphs without otherwise attributing the speaker be assigned
            # implicitly to the same speaker.
            context = dc.replace(
                context,
                speaker=speaker,
                fragment_number=fragment_number,
                fragment_type=fragment_type,
                enclosed_tags=enclosed_tags,
                enclosed_classes=enclosed_classes,
            )

            processed.append((context, paragraph_text))

            # Continue as the leaf nodes are the p/para elements.
            continue

        # Pass through - the set of parent tags for this particular context. This is so
        # we can defer processing a bit further down the track as most of these might
        # only need a present/absent flag to indicate important structure.
        context = dc.replace(
            context, enclosing_tags=context.enclosing_tags | set([tag])
        )

        # Prep children for processing - note we append in reverse order so that the
        # depth first order is preserved.
        children = reversed(element)
        for child in children:
            to_process.append((context, child))

    return transcript_key, transcript_pdf_url, "xml", session_info, processed


def insert_processed_xml_transcript_detail(
    processed_db,
    session_id,
    debate_id,
    url,
    transcript_pdf_url,
    session_info,
    paragraphs,
):
    """
    Insert final processed data into the database.

    """

    # Insert the session details.
    processed_db.execute(
        """
        INSERT into session(session_id, url, transcript_pdf_url, date, chamber) 
            values(?, ?, ?, ?, ?)
        """,
        (
            session_id,
            url,
            transcript_pdf_url,
            session_info["date"],
            session_info["chamber"],
        ),
    )

    # speaker_keys = set()

    last_debate_title = None
    next_debate = debate_id + 1
    debate_no = 1

    for sequence_no, (context, paragraph_text) in enumerate(paragraphs):

        debate_title = "\n".join(c.get("title", "") or "" for c in context.debate_info)

        # Create a new debate sequence whenever the title changes.
        if debate_title is not None and debate_title != last_debate_title:
            processed_db.execute(
                """
                INSERT into debate values(?, ?, ?, ?)

                """,
                (next_debate, session_id, debate_no, debate_title),
            )

            debate_id = next_debate
            next_debate += 1
            debate_no += 1

            last_debate_title = debate_title

        # Normalise speaker_id when present
        speaker_id = context.speaker.get("name.id", None)
        # parliamentary handbook is all uppercase, but transcripts occassionally use
        # lower case, so normalise.
        # Also normalise leading/trailing whitespace while we're at it.
        if speaker_id is not None:
            speaker_id = speaker_id.upper().strip()

        fragment_number = context.fragment_number
        fragment_type = context.fragment_type

        processed_db.execute(
            "INSERT into paragraph values(null, ?, ?, ?, ?, ?, ?, ?)",
            (
                session_id,
                sequence_no,
                speaker_id,
                debate_id,
                fragment_number,
                fragment_type,
                paragraph_text,
            ),
        )

        # NOTE: it's important that we grab the paragraph_id straight after inserting
        # it! If we put another insert before this we will get the wrong identifier.
        para_id = list(processed_db.execute("SELECT last_insert_rowid()"))[0][0]

        processed_db.executemany(
            "INSERT into paragraph_enclosing_tag values(?, ?)",
            ((para_id, tag) for tag in context.enclosing_tags),
        )

        processed_db.executemany(
            "INSERT into paragraph_enclosed_tag values(?, ?)",
            ((para_id, tag) for tag in context.enclosed_tags),
        )

        processed_db.executemany(
            "INSERT into paragraph_enclosed_class values(?, ?)",
            ((para_id, klass) for klass in context.enclosed_classes),
        )

        # Handle some additional contextual information about fragments/procedural
        # units. This is currently a bit weird here, but it's unclear where to fit this
        # otherwise... page numbers and time stamps can be repeated throughout a speech,
        # but first.speech should be a speech (procedural unit) property...
        if speaker_id:

            processed_db.execute(
                # Note: this has no primary key defined yet...
                "INSERT into fragment_speaker_info values(?, ?, ?, ?, ?, ?)",
                (
                    session_id,
                    fragment_number,
                    speaker_id,
                    context.speaker.get("time.stamp", None),
                    context.speaker.get("page.number", None),
                    context.speaker.get("first.speech", None),
                ),
            )

    return next_debate


remove_tags = (
    "chamber.xscript",
    "debate",
    "debate.sub1",
    "link",
    "meta",
    "para",
    "question",
    "row",
    "emsg",
    "graphic",
    "qwn",
    "talk.start",
    "speech",
    "emphasis",
    "spanspec",
    "proctext",
    "sso",
    "interject",
    "break",
    "colspec",
    "tab",
)

remove_tags = re.compile(
    "|".join(f"</?{tag}.*?>" for tag in remove_tags),
    re.IGNORECASE | re.MULTILINE | re.DOTALL,
)

sgml_entity_replacements = {
    "&half;": "½",
    "&frac34;": "¾",
    "&bull;": "•",
    "&yen;": "¥",
    "&mdash;": "—",
    "&cent;": "¢",
    "&frac23;": "⅔",
    "&hyphen;": "-",
    "&frac13;": "⅓",
    "&dagger;": "†",
    "&pound;": "£",
    "&sup2;": "²",
    "&frac14;": "¼",
    "&rsquo;": "’",
    # It's important this comes last - all other entities need to match first.
    # This only appears unescaped in attributes, particularly ministerial roles.
    "&": "&amp;",
}

entity_detector = re.compile("|".join(sgml_entity_replacements.keys()))


def replace_sgml_entity(match):
    return sgml_entity_replacements[match.group(0)]


def chop_sgml_doctype(sgml_str):
    """
    Chop the SGML doctype and associated elements from the start of the document.

    All SGML transcripts have an opening hansard tag.

    """
    start = "<HANSARD"

    return start + sgml_str.partition(start)[2]


def process_sgml_transcript(transcript_key, transcript_pdf_url, sgml_str):
    """
    Extract text units from SGML transcripts, with sufficient information about context.

    """

    # Chop off the (SGML) doctype declaration.
    remove_doctype = chop_sgml_doctype(sgml_str)
    # Brute force, remove tags we know aren't closed just to see what happens when we
    # parse as if this was XML.
    removed_unclosed_tags = remove_tags.sub("", remove_doctype)
    # Replace remaining entities with unicode equivalents
    transformed = entity_detector.sub(replace_sgml_entity, removed_unclosed_tags)

    try:
        root = ET.fromstring(transformed)
    except ET.ParseError as e:
        logger.exception(
            f"Parsing (SGML) error occurred for transcript {transcript_key}. SKIPPING."
        )

    return transcript_key, transcript_pdf_url, "sgml", None, None

    # # Session information first - this is the basic information about the date, house,
    # # etc. and is the same across all of the elements in this transcript.
    # session = root.find("session.header")
    # session_info = {elem.tag: elem.text for elem in session}

    # # The context holds information from the parent elements - it may be added or
    # # extended as processing continues.
    # context = TranscriptContext()

    # # Note we're using a while loop and a queue to avoid Python limitations with
    # # recursion. We still process in depth first order though so we can assign sensible
    # # sequence numbers.
    # sequence_no = 0
    # to_process = [(context, root)]
    # processed = []

    # while to_process:

    #     context, element = to_process.pop()

    #     tag = element.tag

    #     # Extract debate info if present.
    #     new_debate_info = process_debate_info(element)

    #     if new_debate_info:
    #         context = dc.replace(
    #             context,
    #             debate_info=tuple(
    #                 itertools.chain(context.debate_info, new_debate_info)
    #             ),
    #         )

    #     # TODO: speaker information.

    #     # Skip these - they occur only in the newest transcript format and are
    #     # equivalent to the debate_info we've already collected.
    #     elif tag in ("debate.text", "subdebate.text"):
    #         continue

    #     # Finally - the thing we actually care about - the paragraphs of text
    #     # TODO: handle context from the p elements in the newer style transcripts.
    #     elif tag in ("p", "para"):
    #         paragraph_text = " ".join(element.itertext()).strip()
    #         processed.append((context, paragraph_text))
    #         continue

    #     # Pass through - the set of parent tags for this particular context that aren't
    #     # otherwise handled. This is so we can defer processing a bit further down the
    #     # track as most of these might only need a present/absent flag to indicate
    #     # important structure.
    #     else:
    #         context = dc.replace(
    #             context, enclosing_tags=context.enclosing_tags | set([tag])
    #         )

    #     # Prep children for processing - note we append in reverse order so that the
    #     # depth first order is preserved.
    #     children = reversed(element)
    #     for child in children:
    #         to_process.append((context, child))

    # return transcript_key, "sgml", session_info, processed


# The set of known bad transcripts - these are also cases that need further
# investigation
ignore_transcripts = set(
    (
        # The XML transcript is not linked for this date - it ends up processed through
        # the SGML logic which leads to a HTML error page being retrieved...
        "https://parlinfo.aph.gov.au/parlInfo/search/display/display.w3p;query=Id%3A%22chamber%2Fhansards%2F2004-02-10%2F0000%22;src1=sm1",
    )
)


def initialise_database(db: sqlite3.Connection, transcript_rapid_version):
    db.executescript("""
            DROP table if exists paragraph;
            DROP table if exists session;
            DROP table if exists debate;
            DROP table if exists paragraph_enclosing_tag;
            DROP table if exists paragraph_enclosed_tag;
            DROP table if exists paragraph_enclosed_class;
            DROP table if exists rapid_meta;
            DROP table if exists fragment_speaker_info;


            create table session
            (
               session_id integer primary key,
               url unique,
               transcript_pdf_url,
               date       datetime,
               chamber    text
            );

            create table debate
            (
               debate_id  integer primary key,
               session_id integer references session,
               debate_no  integer,
               title,
               unique (session_id, debate_no)
            );

            create table paragraph
            (
               para_id integer primary key,
               session_id references session,
               sequence_number,
               speaker_id,
               debate_id,
               fragment_number,
               fragment_type,
               paragraph_text,
               unique (session_id, sequence_number),
               foreign key (session_id, fragment_number, speaker_id) 
                  references fragment_speaker_info 
            );

            create table fragment_speaker_info
            (
                session_id integer references session,
                fragment_number integer,
                speaker_id,
                time_stamp,
                page_no,
                first_speech
                /* 
                There's also at least: electorate, party, in.gov, and role for
                ministerial appts, but I'm not sure if that fits here. \
                */
                /* 
                This is a candidate key for now - but there seems to be very spotty
                coverage of some fields so let's investigate everything.
                */
                -- primary key (session_id, fragment_number, speaker_id)
            );

            create table paragraph_enclosing_tag
            (
               para_id references paragraph,
               tag,
               primary key (para_id, tag)
            );

            create table paragraph_enclosed_tag
            (
               para_id references paragraph,
               tag,
               primary key (para_id, tag)
            );

            create table paragraph_enclosed_class
            (
               para_id references paragraph,
               class,
               primary key (para_id, class)
            );
               
            CREATE table rapid_meta
            (
               key text primary key,
               value text
            );

            pragma
            journal_mode=WAL;
            """)

    db.execute(f"""
        insert into rapid_meta (key, value) values 
            ('rapid_hansard_version', '{rapid_hansard_version}'),
            ('rapid_hansard_schema_version', '{rapid_hansard_schema_version}'),
            ('transcript_rapid_hansard_version', '{transcript_rapid_version}')
    """)


def get_transcript_list(db: sqlite3.Connection, skip_format=[]) -> sqlite3.Cursor:
    """
    Fetches a list of transcripts to process.

    skip_format is a list that should contain 'sgml' or 'xml' if either or both should be skipped. Use an empty list
    (default) if all formats should be processed.
    """
    conditions = ["retrieved is not null", "transcript_markup is not null"]
    if "sgml" in skip_format:
        conditions.append("transcript_markup_type != 'sgml'")
    if "xml" in skip_format:
        conditions.append("transcript_markup_type != 'xml'")

    where_statement = " and ".join(conditions)

    return transcript_db.execute(f"""
        SELECT 
            url,
            transcript_pdf_url, 
            transcript_markup_type, 
            transcript_markup
        from hansard_transcript
        where {where_statement}
        order by url
        """)


def run_transcript_processing(db: sqlite3.Connection, transcripts):
    db.execute("begin")

    with cf.ProcessPoolExecutor(8) as pool:

        tasks_in_flight = set()

        # We generate session_ids and debate_ids sequentially as surrogate keys.
        # Session IDs map directly to one transcript - debate ids are more complex.
        session_id = 1
        next_debate = 1

        speaker_keys = set()

        for url, transcript_pdf_url, transcript_type, transcript in transcripts:

            if url in ignore_transcripts:
                continue

            if transcript_type == "xml":
                tasks_in_flight.add(
                    pool.submit(
                        process_xml_transcript, url, transcript_pdf_url, transcript
                    )
                )

            elif transcript_type == "sgml":
                tasks_in_flight.add(
                    pool.submit(
                        process_sgml_transcript, url, transcript_pdf_url, transcript
                    )
                )

            if len(tasks_in_flight) > 500:
                completed, tasks_in_flight = cf.wait(
                    tasks_in_flight, return_when=cf.FIRST_COMPLETED
                )

                for task in completed:
                    (
                        url,
                        transcript_pdf_url,
                        transcript_type,
                        session_info,
                        paragraphs,
                    ) = task.result()

                    if transcript_type == "xml":
                        next_debate = insert_processed_xml_transcript_detail(
                            db,
                            session_id,
                            next_debate,
                            url,
                            transcript_pdf_url,
                            session_info,
                            paragraphs,
                        )

                    session_id += 1

                print("Completed:", session_id)

        for task in cf.as_completed(tasks_in_flight):
            (
                url,
                transcript_pdf_url,
                transcript_type,
                session_info,
                paragraphs,
            ) = task.result()
            if transcript_type == "xml":
                next_debate = insert_processed_xml_transcript_detail(
                    db,
                    session_id,
                    next_debate,
                    url,
                    transcript_pdf_url,
                    session_info,
                    paragraphs,
                )
            session_id += 1

    db.execute("commit")


def get_transcript_rapid_version(db: sqlite3.Connection) -> str | None:
    try:
        version = db.execute(
            "select value from rapid_meta where key = 'rapid_hansard_version'"
        ).fetchone()[0]
    except:
        version = None
    return version


if __name__ == "__main__":

    logging.basicConfig(
        filename="prepare_transcripts.log", encoding="utf-8", level=logging.DEBUG
    )

    transcript_db = sqlite3.connect("transcripts_progress.db", isolation_level=None)
    processed_db = sqlite3.connect("oz_federal_hansard.db", isolation_level=None)

    transcript_rapid_version = get_transcript_rapid_version(transcript_db)

    initialise_database(processed_db, transcript_rapid_version)

    ## Fetch the list of transcripts to be processed
    transcripts_list = get_transcript_list(transcript_db)

    run_transcript_processing(processed_db, transcripts_list)
