"""
Download relevant data from the parliamentary handbook source for linking with
data extracted from transcripts.

"""

import sqlite3
import time
from pathlib import Path
import datetime as dt
from click import echo

import requests


def retrieve_parliamentarians(db):

    handbook_api = (
        "https://handbookapi.aph.gov.au/api/individuals?"
        "$orderby=FamilyName,GivenName"
        "&$select=PHID,FamilyName,GivenName,DisplayName,gender,dateOfBirth,dateOfDeath"
    )

    response = requests.get(handbook_api)

    response.raise_for_status()

    parliamentarians = response.json()["value"]

    db.execute("DROP table if exists parliamentarian")
    db.execute("""
        CREATE table parliamentarian (
            phid primary key,
            display_name text,
            family_name text,
            given_name text,
            gender,
            date_of_birth,
            date_of_death
        )
        """)

    db.executemany(
        """
        INSERT into parliamentarian values 
            (:PHID, :DisplayName, :FamilyName, :GivenName, :Gender, :DateOfBirth, :DateOfDeath)

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
    db.execute(
        """
        CREATE temporary table party_member_working (
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

        echo(f"Retrieving party {i+1}/{len(parties)}: {party["PrimaryName"]}")

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
                INSERT into party_member_working
                    values(
                        :party_id,
                        :phid,
                        date(:StartDate),
                        date(:EndDate)
                    )
                """,
                (
                    dict(**row_header, **member_record)
                    for member_record in party_records
                ),
            )



        time.sleep(15)

    # Note on date logic here: the ranges given in the parliamentary handbook are meant
    # to be fully closed intervals, but that leads to some interesting problems when
    # you have people who have a membership in a party for a single day. For example,
    # Brian Burston resigned from a party on 2018-06-18 becoming an independent, then
    # joined another party on 2018-06-19.
    #
    # The start and end dates for his party member records are:
    #
    # 2016-07-02T00:00:00, 2018-06-18T00:00:00
    # 2018-06-18T00:00:00, 2018-06-18T00:00:00
    # 2018-06-18T00:00:00, 2019-06-30T00:00:00
    #
    # Since a parliamentarian can only be a member of a single party at a time, we need
    # to be careful with how we use this data - we can't use the closed range
    # [start_date, end_date] because all three records above would match on 2018-06-18,
    # but we also can't use a semi open range [start_date, end_date) because that would
    # only match the third date range above on 2018-06-18. We need to clean up the data
    # slightly so we can work with a semi open range. The start_date of the third entry
    # would make more sense as 2018-06-19 (unless we were able to model party
    # membership at a sub day) granularity.
    #
    # To make this work: let's treat the ranges as semi open on [start_date, end_date),
    # which will work correctly everywhere except where there are start_dates and
    # end_dates that are the same. We can fix up the records that have the same
    # start/end dates to make this work more easily.

    # Bump end_date when end_date = start_date
    db.execute(
        """
        UPDATE party_member_working
            set end_date = date(end_date, '+1 day')
            where start_date = end_date
        """
    )

    # Adjust start_dates that overlap with the previously adjusted ranges.
    db.execute(
        """
        UPDATE party_member_working
            set start_date = date(start_date, '+1 day')
            where (party_id, phid, start_date) in (
                select
                    pm1.party_id, pm1.phid, pm1.start_date
                from party_member_working pm1
                inner join party_member_working pm2 using(phid)
                where
                    pm2.end_date > pm1.start_date and
                    pm2.end_date <= coalesce(pm1.end_date, '3000-01-01') and
                    pm1.party_id != pm2.party_id
            )
        """
    )

    db.execute("INSERT into party_member select * from party_member_working")

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

        echo(f"Retrieving ministry {i+1}/{len(ministries)}: {ministry["MinistryName"]}")

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
    return dt.datetime.now(dt.UTC).isoformat()


def fetch_all(database_name:str|Path):
    db = sqlite3.connect(database_name, isolation_level=None)

    db.execute("begin")
    # Update info from the parliamentary handbook
    # retrieve_electorates(db)
    retrieve_parliamentarians(db)
    retrieve_party_records(db)
    retrieve_ministries(db)

    db.execute("commit")
