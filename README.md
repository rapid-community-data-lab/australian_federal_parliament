# Reusable and Accessible Proceedings of Australian Federal Parliament

## Installation

Clone or download the source code, and the root folder of the code, in a Python virtual environment (minimum Python 
version 3.12), run `pip install .`

Some functionality requires additional packages which can be installed with appropriate dependency groups:

```
# Extras for downloading transcripts from parlinfo
pip install .[download_hansard]

# Extras for creating parquet exports
pip install .[export_parquet]

```



## Usage

To build a fresh full database of processed transcripts, execute the following (you can change the database 
names/locations as desired):

```shell
rapid_hansard fetch transcripts transcripts.db

rapid_hansard parse transcripts.db rapid_hansard.db

rapid_hansard fetch parliament-data rapid_hansard.db
```

Note that if you are on Linux and you are using Firefox installed via snap, you will need to set an environment variable
to tell the transcript downloader where to find your gecko driver (only necessary for `rapid_hansard fetch transcripts`):

```shell
WEBDRIVER_GECKO_DRIVER=/snap/bin/geckodriver rapid_hansard fetch transcripts transcripts.db
```

Interface commands and options are documented in the help text, to view it run `rapid_hansard --help` or 
e.g. `rapid_hansard parse --help`.
