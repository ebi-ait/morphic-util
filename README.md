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

# Globus Submission Flow (New Storage Backend)

morphic-util now supports submitting datasets and uploading files directly to EMBL-EBI on-prem private storage via Globus.
This section describes the end-to-end user journey.

## 1. Globus Setup (one time only)

Before using morphic-util for uploads, submitters must:

### 1.1 Install Globus CLI
```shell script
pip install globus-cli
globus login
```

### 1.2 Create / use a personal Globus endpoint
```shell script
globus endpoint search "$(hostname)"
globus endpoint local-id
```
Copy this value into:
* `MORPHIC_SRC_COLLECTION_UUID` (environment variable), or
* the CLI config when prompted (`src_collection_uuid`).

### 1.3 Identify the EBI destination endpoint
Your administrator provides a UUID, for example:
```shell script
MORPHIC_EBI_COLLECTION_UUID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
```
This is the private on-prem storage where files will land.

### 1.4 Globus authentication & configuration
#### 1. Log in to Globus once (stores a refresh token locally)
```shell script
morphic-util config-login
```
This will:
* Open an authentication URL (or print it to the terminal)
* Ask you for the auth code
* Store a long-lived refresh token in ~/.morphic-util/config.json
(used automatically for future runs, no need to re-login each time)

#### 2. Register your source collection (where your files live)
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

## 2. Creating a Dataset & Upload Area (via UI or CLI)

In the UI:
* Create a dataset → backend automatically creates an upload folder on the EBI collection (via Globus API).
In the CLI:
```shell script
morphic-util submit --type dataset --file dataset.json --dataset-type raw
morphic-util select <DATASET_ID>
```
The dataset ID = upload area name.

## 3. Uploading Files with Globus

### 3.1 Select active dataset upload area
```shell script
morphic-util select <DATASET_ID>
```

### 3.2 Upload files
```shell script
morphic-util upload myfile.fastq.gz
```

The CLI will:
1. Resolve dataset folder via Provider API
2. Auto-activate Globus endpoints
3. Create missing directories if needed
4. Submit a Globus Transfer Task
5. Stream live progress (bytes, rate, ETA)
6. Confirm completion

### 3.3 List uploaded files
```shell script
morphic-util list
```

### 4. Submitting Metadata After Upload
Once files are uploaded:
```shell script
morphic-util submit-file \
  --file metadata.xlsx \
  --action ADD \
  --dataset <DATASET_ID>
```
This registers biomaterials, protocols, library preps, and sequencing files.

### 5. Globus-backed delete
The delete command can use the Provider API + Globus backend for asynchronous deletion of files from the dataset’s upload area.
```shell script
morphic-util delete -g -a
morphic-util delete -g path/to/file1 path/to/file2
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

### 5. Full User Journey Summary
1. Login / configure morphic-util 
2. Ensure Globus local endpoint is available 
3. Create dataset (UI or CLI) → backend creates upload folder on EBI private storage 
4. Select dataset as upload target 
5. Upload files via Globus 
6. Verify uploaded files 
7. Submit metadata referencing uploaded files