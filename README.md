# morphic-util

CLI tool for submitting analysis data and metadata

# Users

## Prerequisites

Users need to have

1. Basic command-line knowledge
2. Python 3.10 installed on their machine
3. AWS Cognito username or email and password

## Install

The [morphic-util](https://pypi.org/project/morphic-util/) tool is available to install from PyPi.

```shell script
$ pip install morphic-util
```

## Usage

Display help and list of commands.

```shell script
$ morphic-util -h
usage: morphic-util [-h] [--version] [--profile PROFILE] {config,create,select,list,upload,download,delete} ...

morphic-util

optional arguments:
  -h, --help            show this help message and exit
  --version, -v         show program's version number and exit

command:
  {config,submit,submit-file,create,select,list,upload,download,delete}
    config              configure AWS credentials
    submit              submit your study, dataset or biomaterials metadata (incomplete as all metadata types is not supported yet, expected to be completed on August 2024)
    submit-file         submit your metadata file with cell lines, differentiated products, library preparations, sequencing files, 
                        and optionally context-specific data (e.g., pooled or unperturbed experiments)    
    create              create an upload area (authorised users only)
    select              select or show the active upload area
    list                list contents of the area
    upload              upload files to the area
    download            download files from the area
    delete              delete files from the area
```

In the above, optional arguments are between `[]` and choices between `{}`.

The basic usage is as follows:

```shell script
$ morphic-util cmd ARG1 ARG2 -o1 -o2
```

Use the tool by specifying a command (`cmd` - see list below) to run, any mandatory (positional) arguments (e.g. `ARG1`
and `ARG2` - see positional args for each command), and any optional arguments (e.g. `-o1` and `o2` - see options for
each command).

### What’s new

**Automatic clonal-cell-line reuse** – if a clonal cell-line label in your
spreadsheet already exists in the ingest database, `morphic-util` will detect
it and link to the existing record instead of creating a duplicate.

**Configurable ingest endpoint** – set the environment variable
`INGEST_API_BASE` in `spreadsheet_util.py:parse_cell_lines` (defaults to `https://api.ingest.archive.morphic.bio`) to
target a different ingest deployment without editing code.

## Commands

Help with specific command:

```shell script
$ morphic-util <cmd> -h
```

Some commands or options/flags are restricted to authorised users (admin) only.

## `config` command

Configure AWS credentials

```shell script
$ morphic-util config username password

positional arguments:
  username         AWS Cognito username
  password         AWS Cognito password
```

The tool uses the profile name _morphic-util_ in local AWS config files.

## `submit` command
Submit your study and dataset metadata and create your AWS upload area for uploading data files

```shell script
positional arguments:
$ morphic-util submit --type <TYPE> --file <PATH_TO_FILE> [--study <STUDY_ID>] --dataset-type <DATASET_TYPE> [--derived-from <PARENT_IDS>]

  Required:
    --type: type of metadata being submitted (e.g. study or dataset)
    --file: path to the file containing the metadata

  Required for datasets:
    --dataset-type: Dataset type (e.g., raw, filtered, processed, analysis)
    
  Conditionally required for datasets:
    --derived-from: Comma-separated list of dataset IDs this dataset is derived from

  Optional (for datasets):
    --study: Link the dataset to an existing study
   
  Validation rules (for datasets):
    raw: Must not include --derived-from
    filtered, processed: Must be derived from a raw dataset
    analysis: Must be derived from a processed dataset
```

## `submit-file` command
Submit your study and dataset metadata and create your AWS upload area for uploading data files

```shell script
positional arguments:
$ morphic-util submit-file --file <PATH_TO_FILE> --action <SUBMISSION_ACTION> --dataset <the analyis which has generated the data and the metadata> [--context <CONTEXT>]

positional arguments:
  --file         path to the file containing the metadata
  --action       ADD, MODIFY or DELETE based on the type of submission
  --dataset      the identifier for the analysis
  
optional arguments:
  --context      optional ingestion context, e.g.:
                   'pooled_differentiated' → for MSK pooled datasets
                   'unperturbed_multiple' → for UCSF datasets
                 If omitted, legacy behavior is used
```
Example usage:
```shell script
morphic-util submit-file --file my_file.xlsx --action ADD --dataset 67f8519e68005a3744c40fcf --context pooled_differentiated
```

## `create` command

Create an upload area/ project folder **(authorised users only)**

```shell script
$ morphic-util create NAME [-p {u,ud,ux,udx}]

positional arguments:
  NAME               name for the new area/ project folder

optional arguments:
  -p {u,ud,ux,udx}   allowed actions (permissions) on new area. u for
                     upload, x for delete and d for download. Default is ux
```

## `select` command

Show or select the active upload area/ project folder

```shell script
$ morphic-util select AREA

positional arguments:
  AREA                area name/ folder name. 
```

If AREA is not specified, the selected area is shown.

## `list` command

List contents of selected area

```shell script
$ morphic-util list [-b]

optional arguments:
  -b                 list all areas in bucket **(authorised users only)**
```

## `upload` command

Upload files to the selected area

```shell script
$ morphic-util upload PATH [PATH ...] [-o]

positional arguments:
  PATH               valid file or directory

optional arguments:
  -o                  overwrite files with same names
```

## `download` command

Download files from the selected area **(authorised users only)**

```shell script
$ morphic-util download (-a | -f file [file ...])

optional arguments:
  -a                  download all files from selected area
  -f file [file ...]  download specified file(s) only
```

## `delete` command

Delete files from the selected area

```shell script
$ morphic-util delete [-a | -d] [PATH [PATH ...]]

positional arguments:
  PATH               path to file or directory to delete

optional arguments:
  -a                 delete all files from the area
  -d                 delete upload area and contents (authorised users only)
```
## Performing a submission
### Authenticate
```shell script
$ morphic-util config username password

positional arguments:
  username         AWS Cognito username
  password         AWS Cognito password
```
### Create your study
```shell script
positional arguments:
$ morphic-util submit --type study --file <PATH_TO_STUDY_METADATA_FILE>

  --type         type of metadata being submitted (here it is study)
  --file         path to the file containing the metadata
```
### Create your dataset and link it to your study
```shell script
positional arguments:
$ morphic-util submit --type dataset --file <PATH_TO_DATASET_METADATA_FILE> [--study <STUDY_ID>] [--dataset-type <TYPE>] [--derived-from <PARENT_IDS>]
  --type         type of metadata being submitted (here it is dataset)
  --file         path to the file containing the metadata (optional)
  --study        STUDY_ID obtained in the last step
  --dataset-type: One of raw, filtered, processed, or analysis (required)
  --derived-from: Comma-separated list of dataset IDs this dataset is derived from (required for all except raw)

  Validation rules:
    raw: Must not have --derived-from
    filtered or processed: Must be derived from raw
    analysis: Must be derived from one or more processed datasets
```
### `select` your upload area to upload your data files (the upload area name is same as your DATASET_ID)
Show or select the data file upload area
```shell script
$ morphic-util select AREA

positional arguments:
  AREA                upload area name (same as DATASET_ID obtained in the last step). 
```
### `upload` your data files
Upload files to the selected area for the dataset
```shell script
$ morphic-util upload PATH [PATH ...] [-o]

positional arguments:
  PATH               valid file or directory

optional arguments:
  -o                  overwrite files with same names
```
### `list` uploaded data files to verify that data file upload has been successful
```shell script
$ morphic-util list
```
### `submit-file` command to submit your dataset metadata containing your biomaterials, processes, protocols and files
```shell script
positional arguments:
$ morphic-util submit-file --file <PATH_TO_FILE> --action <SUBMISSION_ACTION> --dataset <the analyis which has generated the data and the metadata>

  --file         path to the file containing the metadata
  --action       ADD, MODIFY or DELETE based on the type of submission
  --dataset      the identifier for the analysis
```

# Developers

Download dependencies

```
pip install -r requirements.txt
```

Run tests

```shell script
nosetests
```

# Globus Submission Flow (Test release)

morphic-util now supports submitting datasets and uploading files directly to EMBL-EBI on-prem private storage via Globus.
This section describes the end-to-end user journey for the Globus-enabled test release.

* Expected version for testers: `morphic-util 1.1.0rc1`
* This version is distributed via TestPyPI for initial DPC testing.

## Testing morphic-util (Globus-enabled branch)

This guide is for DPC users and collaborators testing the current Globus-enabled version of morphic-util.

It assumes:
* no prior local setup, and
* use of a clean Python environment (strongly recommended).

### Prerequisites

Users need:
* Python ≥ 3.10
* pip available
* Internet access
* A Globus account
(institutional login or ORCID-backed account both work)

Verify locally:
```shell script
python3 --version
pip --version
```
### Create a clean environment (strongly recommended)

⚠️ This step is important.
Testing has shown that existing virtual environments or globally installed packages can cause subtle issues.
```shell script
mkdir ~/morphic-test
cd ~/morphic-test
```
```shell script
python3 -m venv venv
source .venv/bin/activate
```

Verify:
```shell script
python -V
pip -V
```

You should now be inside a clean virtual environment.

### Install morphic-util

#### Option A — Install from GitHub branch (current reality) 
This is how internal users should install today:
```shell script
pip install git+https://github.com/ebi-ait/morphic-util.git@feature/globus-api-integration
```

#### Verify installation
```shell script
morphic-util --version
morphic-util --help
```
Expected:
* No traceback
* Version shown: 1.0.5
* Commands include:
`config-globus, globus-login, submit, submit-file, create, select, upload, list, delete, sync, …---`

## 1. Globus Submission Flow – Source Collection Setup

This guide explains how to identify and configure your **Globus source collection**, which is required for uploading files to **MorPhiC private storage** using Globus.

The setup described here is **one-time only**.

---

### What is a “source collection”?

A **source collection** is the Globus collection where your data already lives.

This may be:
- an **institutional HPC or shared server** (most common), or
- a **local machine or personal VM** (via Globus Connect Personal).

`morphic-util` requires the **collection UUID** so it can instruct Globus to transfer files from your location to the MorPhiC private storage.

---

### Choose the correct setup path

#### Use **Option A** if:
- your data lives on an HPC, shared server, or institutional storage
- you have already transferred data using Globus in the past
- you can already browse your data in the Globus web interface

➡️ **No additional installation required**

#### Use **Option B** if:
- your data lives on your laptop or a personal VM
- you do **not** already have a Globus collection

➡️ **Requires Globus Connect Personal (GCP)**

---

### Option A — Institutional Globus collection (GCS / HPC / server)

This is the **recommended and most common** case for DPCs.

If you have previously uploaded data from an HPC or institutional server using Globus, you already have a valid source collection.

#### A1. Retrieve the source collection UUID (Web UI)

1. Open the Globus web app: https://app.globus.org
2. Log in using your institutional or ORCID-backed Globus account.
3. Open **File Manager**.
4. In the collection selector (top left):
    - search for your institutional endpoint, or
    - select a collection you have used before
5. Click the **ⓘ (information)** icon next to the collection name.
6. Copy the **Collection UUID**.

#### A2. Retrieve the source collection UUID (CLI)

```bash
globus login
globus whoami
```

#### Method 1 — From recent transfers

```bash
globus task list --limit 10
globus task show <TASK_ID>
```

Look for **Source Endpoint ID**.

#### Method 2 — Search by name

```bash
globus endpoint search "HPC"
globus endpoint search "Institute"
globus collection list <ENDPOINT_ID>
```

---

### Option B — Globus Connect Personal (GCP)

Use this option **only if your data is not already on an institutional Globus collection**.

#### B1. Install and configure GCP

1. Install Globus Connect Personal: https://www.globus.org/globus-connect-personal
2. Start Globus Connect Personal.
3. Expose the directory that contains your data (e.g. `~/data`, `/mnt/storage/project_x`).

> ⚠️ Only directories explicitly exposed during setup will be accessible.

#### B2. Retrieve the source collection UUID (CLI)

```bash
globus endpoint local-id
```

Optional:

```bash
globus endpoint search "$(hostname)"
```
---

### Summary

| Where your data lives | Setup option | Installation required |
|----------------------|--------------|------------------------|
| Institutional HPC / shared server | Option A (GCS) | ❌ No |
| Previously used Globus | Option A (GCS) | ❌ No |
| Laptop / personal VM | Option B (GCP) | ✅ Yes |
---
## 2. Authentication & Configuration (Globus-only — Cognito no longer required)
As of the new Globus integration, morphic-util no longer uses or requires AWS Cognito.
All authentication for both:
* Provider API access
* File upload/delete using Globus Transfer

is done through Globus Native App Authentication.

Submitters now only need to log in to Globus once.
No Cognito username/password is needed anymore.

### 2.1 Globus Authentication (required for file transfers)
```shell script
morphic-util globus-login
```
This will:
* Open an authentication URL (or print it to the terminal)
* Ask you for the auth code
* Store a long-lived refresh token in ~/.morphic-util/config.json
(used automatically for future runs, no need to re-login each time)

### 2.2 Register your source collection (where your files live)
```shell script
morphic-util config-globus --src-collection-uuid <YOUR_SOURCE_COLLECTION_UUID>
```
* <YOUR_SOURCE_COLLECTION_UUID> is your personal/desktop endpoint (e.g. Globus Connect Personal)
* This value is saved in ~/.morphic-util/config.json under src_collection_uuid
* Destination collection, API URL, and dest root are preconfigured by the service admin (via env/defaults)

Configuration is stored at:
```shell script
~/.morphic-util/config.json
```
---
## 3. Creating a Study, Dataset & Upload Area
### ⚠️ Important: User role requirement

When you first authenticate with **morphic-util**, your account is registered as a **Guest**.
- **Guests can** authenticate and use Globus
- **Guests cannot** create studies, datasets or upload data
- Attempting to create a dataset as a Guest will return **403 Forbidden**

To create datasets and upload files, your account must be upgraded to a **Contributor/ Wrangler**.

➡️ If you encounter a **403 Forbidden** error when creating a dataset, please contact the MorPhiC team (e.g. **alexkoci@ebi.ac.uk**) to have your role upgraded.

### 3.1 Create a Study (optional but recommended)
```shell script
morphic-util submit --type study --file study.json
```
Example `study.json` (can be minimal):
```shell script
{
    "title": "Sample Study",
    "description": "Example study created via morphic-util",
    "contributors": ["Name Surname"],
    "institution": "EMBL-EBI"
}
```
ℹ️ You can create datasets without a study and link them later.

### 3.2 Create a Dataset & Upload Area (with optional Study linking)

If you already have a Study ID, you can link the dataset at creation time:
```shell script
morphic-util submit \
--type dataset \
--file dataset.json \
--study <STUDY_ID> \
--dataset-type raw
```
- The dataset ID is also the upload area name

Example `dataset.json`:
```json
{
  "title": "Sample Processed Dataset",
  "description": "A processed dataset derived from raw data.",
  "contributors": ["Name Surname"],
  "institution": "EMBL-EBI"
}
```
### 3.3 Create a Dataset & Upload Area (without linking to a Study)
```shell script
morphic-util submit --type dataset --file dataset.json --dataset-type raw
```
- When prompted to link to a study, you may press Enter to skip
---
## 4. Uploading Files with Globus

### 4.1. Select active dataset upload area
```shell script
morphic-util select <DATASET_ID>
```

### 4.2. Upload a single file
```shell script
morphic-util upload myfile.fastq.gz
```

### 4.3 Upload a folder recursively (bulk upload)
You can upload all files within a directory in one command:
```shell script
morphic-util upload path/to/files/
```
This will recursively discover and upload all files under the given folder.

The CLI will:
1. Resolve dataset folder via Provider API
2. Auto-activate Globus endpoints
3. Submit a Globus Transfer Task
4. Stream live progress (bytes, rate, ETA)
5. Confirm completion
---
#### ⚠️ Important constraint on source paths

All files and directories you upload must be located under the source root directory configured during Globus setup
(for example, the root of your GCP or local Globus collection).

- Paths outside the configured Globus collection cannot be accessed
- Absolute paths outside the collection will fail
- Relative paths are resolved within the configured collection root
---

### 4.4. List uploaded files
```shell script
morphic-util list
```

## 5. Submitting Metadata After Upload
Once files are uploaded:
```shell script
morphic-util submit-file \
  --file metadata.xlsx \
  --action ADD \
  --dataset <DATASET_ID>
```
This command performs metadata validation, cross-file validation, and (if successful) ingests the submission into the MorPhiC database, by registering biomaterials, protocols, library preps, and sequencing files.

Notes:
- `metadata.xlsx` is the spreadsheet containing the dataset metadata.
- An example spreadsheet for testing purposes is available [here](https://docs.google.com/spreadsheets/d/1zoRYWwzqoh2Qa17P_tb227rsbZbh267k/edit?gid=45802078#gid=45802078)

### 5.1 What happens during submission

The submission process consists of the following steps:

#### 1.  Metadata content validation
The metadata spreadsheet is first validated for internal consistency and completeness, including:

* required sheets and columns
* valid entity relationships (e.g. biomaterials → library preparations → sequencing files)
* correct identifiers and references between entities

#### 2. Cross-file validation (metadata ↔ uploaded files)

The tool then verifies that files referenced in the spreadsheet are present in the dataset’s upload area on the Globus private storage.

This includes checks for:
* sequencing files listed in the spreadsheet but missing from storage
* uploaded files that are not referenced in the metadata

#### 3. Successful validation and ingestion
If all validation steps succeed:

* the metadata is ingested into the MorPhiC database
* the submitted metadata.xlsx is copied into the dataset upload area
* a submission results spreadsheet (e.g. validation summary) is generated and uploaded alongside it

The dataset is then ready for downstream processing.

## 6. Globus-backed delete
The delete command can use the Provider API + Globus backend for asynchronous deletion of files from the dataset’s upload area.
```shell script
morphic-util delete -g -a
morphic-util delete -g file1 file2
```
Options:
* `-g, --globus`
Use the Globus-backed Provider API delete:
  * Submits an async Globus delete task
  * Prints a summary and task id (server-side)
* `-a`
Delete all contents of the current dataset area
* `PATH [...]`
Delete specific file(s) or subpaths within the dataset area

## 7. Full User Journey Summary
1. Authenticate with Globus (one-time login).
2. Configure the Globus source collection (local machine via GCP or institutional endpoint).
3. Create a Dataset (via UI or CLI) — this provisions the upload area on EMBL-EBI private storage.
4. Select the Dataset as the active upload target.
5. Upload data files via Globus (`morphic-util upload`).
6. Verify uploaded files (`morphic-util list` or via the UI).
7. Submit spreadsheet metadata referencing the uploaded files.