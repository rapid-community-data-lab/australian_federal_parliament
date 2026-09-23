# Building and updating RAPID Hansard datasets

> [!NOTE]
> Installation instructions for the rapid_hansard command-line utility are covered in [README.md](README.md). Also see 
the readme for usage instructions and troubleshooting.



The published RAPID-CDL Hansard datasets are built from two internal working SQLite databases:

- The transcript database (default name `transcripts.db`), which contains the original sitting day transcript files, as 
  well as metadata and other materials required when crawling the Parliament House website for transcripts.
- The processed database (default name `rapid_hansard.db`), which contains the parsed sitting day transcripts, and 
  additional data from the Parliamentary Handbook

Once the working databases are built and/or updated, any or all of the output datasets can be exported.

## Creating and updating working databases

If no transcript database exists, a new one will be created and all transcripts (from 1901-present) will be fetched and 
stored. If a transcript database already exists, only new or updated transcripts will be fetched and added to the 
database.

Unlike the transcript database, processing the downloaded transcripts with `rapid_hansard parse` will overwrite 
existing data. This means that it is possible for inconsistencies in the data to arise if transcripts are parsed into 
an existing database but the Parliamentary Handbook data is not refreshed, or vice versa.

Whether databases already exist or not, the sequence of commands to build/update the working databases is the same:

```shell
rapid_hansard fetch transcripts transcripts.db

rapid_hansard parse transcripts.db rapid_hansard.db

rapid_hansard fetch parliament-data rapid_hansard.db
```

#### Dataset update checklist

- [ ] Fetch transcripts (`rapid_hansard fetch transcripts`)
- [ ] Parse transcripts (`rapid_hansard parse`)
- [ ] Fetch Parliamentary Handbook data (`rapid_hansard fetch parliament-data`)

## Exporting Datasets

### Analytical dataset

Zenodo record: https://zenodo.org/records/22868754

The analytical dataset is an export of the parsed transcript data plus Parliamentary Handbook data in the
[Parquet](https://parquet.apache.org/) format, for using in computational workflows or importing into computational 
tools for analysis.

Sample export command:

```shell
rapid_hansard export parquet rapid_hansard.db export_parquet/
```

#### Analytical dataset update checklist

- [ ] Run data export to produce parquet files
- [ ] Create a new version of the Zenodo record and upload the updated files

### Transcript dataset

Zenodo record: TBA

The full transcript dataset is the original sitting day transcripts bundled together with metadata and speaker lists.

Sample export command:

```shell
rapid_hansard export transcripts transcripts.db rapid_hansard.db hansard_transcripts/
```

This will produce a nested folder structure with transcripts, metadata, and speaker lists - there will be a great many
separate files.

#### Transcript dataset update checklist

- [ ] Run data export
- [ ] (recommended) create a zip file of the exported data folder
- [ ] Create a new version of the Zenodo record, upload the zip file, and upload the `summary.txt` file in the data 
      folder as well so it can be read in the Zenodo listing.
