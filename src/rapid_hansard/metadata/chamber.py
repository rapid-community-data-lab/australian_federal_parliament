"""
In addition to defining a `Chamber` class to represent chambers of parliament, this module declares constants
for the two chambers of the Australian Parliament (as that is the only parliament `rapid_hansard` currently works with):
`SENATE` and `H_OF_REPS` (for the House of Representatives, which is a very long name).

It also declares a utility `CHAMBER_NAME_MAP`, which maps the known variants of the names of each chamber in the dataset
to the `SENATE` and `H_OF_REPS` objects as appropriate. For example, if a `row['chamber']` value fetched from the database is
"House Hansard", evaluating `CHAMBER_NAME_MAP[row['chamber']].name` will produce "House of Representatives".
"""
from dataclasses import dataclass, field
from typing import Set

from logging import getLogger


logger = getLogger(__name__)


@dataclass()
class Chamber:
    """
    A utility representation of a chamber of parliament and its name variants.

    The primary `name` will be added to the `name_variants` set on object initialisation, and all the name variants
    will be converted to lowercase.

    Example of possible usage:
    ```python
    from rapid_hansard.metadata.chamber import H_OF_REPS

    if database_result_row['chamber'].lower() in H_OF_REPS.name_variants:
        data['chamber'] = H_OF_REPS
    ```
    """
    name: str
    name_variants: Set[str] = field(default_factory=set)

    def __post_init__(self):
        if self.name not in self.name_variants:
            self.name_variants.add(self.name)

        self.name_variants = set(map(lambda s: s.lower(), self.name_variants))

    def name_map(self):
        return {v: self for v in self.name_variants}


SENATE = Chamber("Senate", {"SEN"})

H_OF_REPS = Chamber("House", {"House of Reps", "REPS", "House of Representatives", "House Hansard"})


def map_name_variations(chamber_1: Chamber, chamber_2: Chamber):
    try:
        assert chamber_1.name_variants.isdisjoint(chamber_2.name_variants)
    except AssertionError:
        logger.error(f"Data problem: one name can't refer to more than one chamber of parliament. "
                     f"Duplicate name(s): {chamber_1.name_variants.intersection(chamber_2.name_variants)}")
        raise

    return chamber_1.name_map() | chamber_2.name_map()


CHAMBER_NAME_MAP = map_name_variations(SENATE, H_OF_REPS)
