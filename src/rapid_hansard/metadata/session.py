from dataclasses import dataclass
from typing import Collection, Any
import datetime as dt
from rapid_hansard.metadata.chamber import Chamber

from rapid_hansard import __version__

default_method = (
    "The original transcript from ParlInfo (https://parlinfo.aph.gov.au) was downloaded and processed with "
    "the rapid_hansard (https://github.com/rapid-community-data-lab/australian_federal_parliament) utility "
    f"version {__version__} by RAPID-CDL (https://rapid-cdl.edu.au). "
    f"This dataset was produced on {dt.date.today()}. See the individual sessions for details of from where and when "
    "each original file was downloaded."
)


@dataclass
class License:
    text: str
    url: str | None = None


@dataclass
class Session:
    id: str
    parliament: str
    chamber: Chamber
    original_source: str
    original_retrieved_at: str
    speakers: Collection[Any] | None


@dataclass
class SessionDataset:
    title: str
    version: str
    sessions: Collection[Session]
    license: License
    uri: str|None = None
    processing_method: str = default_method
