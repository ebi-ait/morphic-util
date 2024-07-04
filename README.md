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
    submit-file         submit your metadata file containing your cell lines, differentiated cell lines, library preparations and sequencing files
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
$ morphic-util submit --type <TYPE> --file <PATH_TO_FILE>

  --type         type of metadata being submitted (e.g. study or dataset)
  --file         path to the file containing the metadata
```

## `submit-file` command
Submit your study and dataset metadata and create your AWS upload area for uploading data files

```shell script
positional arguments:
$ morphic-util submit-file --file <PATH_TO_FILE> --action <SUBMISSION_ACTION> --dataset <the analyis which has generated the data and the metadata>

  --file         path to the file containing the metadata
  --action       ADD, MODIFY or DELETE based on the type of submission
  --dataset      the identifier for the analysis
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

# Developers

Download dependencies

```
pip install -r requirements.txt
```

Run tests

```shell script
nosetests
```
