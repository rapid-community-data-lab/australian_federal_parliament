"""
Download relevant data from the parliamentary handbook source for linking with
data extracted from transcripts.

"""

# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "requests"
# ]
# ///

import sqlite3
import time

import requests


def retrieve_parliamentarians(db):

    handbook_api = (
        "https://handbookapi.aph.gov.au/api/individuals?"
        "$orderby=FamilyName,GivenName"
        "&$select=PHID,DisplayName,gender,dateOfBirth,dateOfDeath"
    )

    response = requests.get(handbook_api)

    response.raise_for_status()

    parliamentarians = response.json()["value"]

    db.execute("DROP table if exists parliamentarian")
    db.execute("""
        CREATE table parliamentarian (
            phid primary key,
            display_name text,
            gender,
            date_of_birth,
            date_of_death
        )
        """)

    db.executemany(
        """
        INSERT into parliamentarian values 
            (:PHID, :DisplayName, :Gender, :DateOfBirth, :DateOfDeath)

        """,
        parliamentarians,
    )


def retrieve_party_records(db):
    """
    Retrieve parties and membership information from the Parliamentary Handbook.

    Parliamentarian membership in a party is recorded as part of the records for each
    party - so we need to retrieve the complete list of all parties that exist, then
    iterate through them to get a complete record of the history for each
    parliamentarian.

    """

    db.execute("DROP table if exists party")
    db.execute(
        """
        CREATE table party (
            party_id integer primary key,
            name text
        )
        """,
    )
    db.execute("DROP table if exists party_member")
    db.execute(
        """
        CREATE table party_member (
            party_id integer references party,
            phid references parliamentarian,
            start_date,
            end_date,
            primary key (party_id, phid, start_date)
        )
        """,
    )

    all_parties = requests.get("https://handbookapi.aph.gov.au/api/partiesdata/parties")

    all_parties.raise_for_status()

    parties = all_parties.json()

    db.executemany("INSERT into party values (:PartyID, :PrimaryName)", parties)

    detailed_url = "https://handbookapi.aph.gov.au/api/partiesdata/partydetailed"
    for i, party in enumerate(parties):

        party_id = party["PartyID"]

        print(f"Retrieving party {i+1}/{len(parties)}:", party["PrimaryName"])

        data = {"partyID": party_id}

        party_detailed = requests.get(detailed_url, params=data)

        party_detailed.raise_for_status()

        party_members = party_detailed.json()["PartyMembers"]

        for member in party_members:
            row_header = {"party_id": party_id, "phid": member["PHID"]}

            # A person can have multiple records in a party, representing: losing and
            # regaining their seat, leaving/joining a party, changing from the house to
            # the senate etc.

            party_records = member["PartyRecords"]

            db.executemany(
                """
                INSERT into party_member values(:party_id, :phid, :StartDate, :EndDate)
                """,
                (
                    dict(**row_header, **member_record)
                    for member_record in party_records
                ),
            )

        time.sleep(15)


def retrieve_ministries(db):
    """
    Retrieve ministries and ministerial appointments.

    Like parties, this is a two part process: we need to iterate through all ministries
    as they're recorded as discrete events, and who is assigned at the time of each
    ministry.

    """

    db.execute("DROP table if exists ministry")
    db.execute(
        """
        CREATE table ministry (
            ministry_id integer primary key,
            name,
            start_date date,
            end_date date
        )
        """,
    )
    db.execute("DROP table if exists minister")
    db.execute(
        """
        CREATE table minister (
            phid,
            role,
            preposition,
            entity,
            start_date,
            end_date,
            primary key (phid, start_date, role, entity)
        )
        """,
    )

    all_ministries = requests.get(
        "https://handbookapi.aph.gov.au/api/StatisticalInformation/Ministries"
    )

    all_ministries.raise_for_status()

    ministries = all_ministries.json()

    db.executemany(
        "INSERT into ministry values (:Id, :MinistryName, :DateStart, :DateEnd)",
        ministries,
    )

    detailed_url = (
        "https://handbookapi.aph.gov.au/api/ministryrecords?$filter=MID%20eq%20{}"
    )
    for i, ministry in enumerate(ministries):

        ministry_id = ministry["Id"]

        print(f"Retrieving ministry {i+1}/{len(ministries)}:", ministry["MinistryName"])

        ministry_detailed = requests.get(detailed_url.format(ministry_id))

        ministry_detailed.raise_for_status()

        ministry_roles = ministry_detailed.json()["value"]

        for m in ministry_roles:
            if m["RDateEnd"] == "":
                m["RDateEnd"] = None

        # Note that this is replace into, because the records for each member in
        # a ministry also include consecutive service from an earlier ministry.
        db.executemany(
            """
            REPLACE into minister 
                values(:PHID, :Role, :Prep, :Entity, :RDateStart, :RDateEnd)
            """,
            ministry_roles,
        )

        time.sleep(15)


def retrieve_electorates(db):
    """Retrieve electorate information from the Parliamentary Handbook."""
    pass


def timestamp_now():
    return datetime.datetime.now(datetime.UTC).isoformat()


if __name__ == "__main__":

    import os
    import sys

    args = sys.argv[1:]

    db = sqlite3.connect("oz_federal_hansard.db", isolation_level=None)

    db.execute("begin")
    # Update info from the parliamentary handbook
    # retrieve_electorates(db)
    retrieve_parliamentarians(db)
    retrieve_party_records(db)
    retrieve_ministries(db)

    db.execute("commit")
