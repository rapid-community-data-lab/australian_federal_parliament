# Speeches in Australian Federal Parliament on the Job-Ready Graduates program

The Job-Ready Graduates reforms were [introduced to Federal Parliament by the Morrison Government on 26 August 2020](https://www.aph.gov.au/Parliamentary_Business/Hansard/Hansard_Display?bid=chamber/hansardr/e30437e2-4793-4a18-91ef-dc9e7c8273f1/&sid=0022) and [passed with amendments on 19 October 2020](https://www.aph.gov.au/Parliamentary_Business/Hansard/Hansard_Display?bid=chamber/hansardr/7bf0b0b3-e260-451a-aa4e-ec6d77176285/&sid=0241).

In light of [Senator Mehreen Faruqi's proposed bill](https://www.aph.gov.au/Parliamentary_Business/Hansard/Hansard_Display?bid=chamber/hansards/28885/&sid=0131) to repeal the substantive changes of the Jobs-Ready Graduates reforms we've prepared this dataset of speeches in Federal Parliament about the Jobs-Ready Graduates reforms. Currently there is a [Senate inquiry into this proposed bill](https://www.aph.gov.au/Parliamentary_Business/Committees/Senate/Education_and_Employment/ReverseJRG), accepting submissions until 10 April 2026. We hope this collection of speeches support people who are considering making a submission to this inquiry by collating what has already been said in Federal Parliament about this legislation, its aims, and its predicted impacts. 

This dataset includes all text from speeches between 2020-01-01 and 2026-03-05 that:

- use the phrase "job-ready graduates", or
- are used in a procedural context where the title of the debate includes "job-ready graduates"

Text matching was done in a case-insensitive manner, and the hyphen was optional, so "Job-ready graduates", "job ready graduates", and "JOB READY GRADUATES" are all matched.

This is designed to capture most of the explicit discussion about the Job-Ready Graduates legislation. This includes specific discussion of the bill during the legislative process (particularly the second reading speeches), procedural elements of the legislative process such as referring the bill to a committee and the subsequent report, amendments made in the senate, and statements by elected representatives on the nature and impact of the bill after its passage into law.

This dataset was prepared using the approach documented in [our Github repository](https://github.com/rapid-community-data-lab/australian_federal_parliament). The final dataset was prepared using [this exact script](https://github.com/rapid-community-data-lab/australian_federal_parliament/blob/v0.0.1/example_scripts/job_ready_graduates/job_ready_graduates.py). In brief this means:

1. Downloading all of the XML transcripts of each session of the House of Representatives or the Senate.
2. Extracting the paragraphs of text from each transcript, along with information about who was speaking and the procedural context.
3. Joining this information about spoken units with information from the [Parliamentary Handbook](https://handbook.aph.gov.au/) to identify speakers and their parties.
4. Performing a simple string match on the text of speeches and the debate title to identify relevant speeches.
5. Creating the final Excel file included in this dataset.


## About the dataset/what's included

The data is distributed as an Excel spreadsheet containing 9 columns.

Each row in the spreadsheet corresponds to one 'paragraph' as marked up in the source transcripts provided by the Parliamentary Library. This is the smallest unit of text that can reliably attributed to a single speaker.

The dataset includes the following attributes:

| attribute  | description |
|-------|-----|
|*date* | the date of the session |
|*chamber* | whether this session was for the House of Reps or the Senate |
|*full_transcript_link* | the link to the official (full day) transcript corresponding to this row |
|*debate_title* | the title of the debate, indicating the procedural context in which the speech occured |
|*speech_number* | the speech number within that session (date/house) |
|*speaker* | The name of the speaker who has the procedural floor, or 'interjector' if the speaking is interrupted (including by the Speaker of the House or President of the Senate). This will be blank for certain procedural or descriptive text, such as noting that division was occuring. |
|*party* | The party the speaker belongs to (at the time of speaking) |
|*paragraph_text* | The paragraph of transcribed text (removing all other markup) |
|*matches_phrase* | 1 if this paragraph contains the phrase 'job-ready graduates' (case-insensitive), 0 otherwise. |

Rows with the same date, chamber and speech_number correspond to the same speech.

# License

Note that this dataset is licensed CC-BY-NC-ND, as under the [original license of the data](https://www.aph.gov.au/Help/Disclaimer_Privacy_Copyright#c) released by Australian Parliament.