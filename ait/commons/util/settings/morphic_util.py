from pathlib import Path

NAME = 'morphic-util'
VERSION = '0.0.10'
DESC = 'CLI tool for uploading data to Morphic AWS S3 bucket'
AUTHOR = 'morphic-bio-dev'
AUTHOR_EMAIL = 'dgupta@ebi.ac.uk'

# when true, displays exception details; otherwise user-friendly error message
DEBUG_MODE = False

DIR_SUPPORT = False

MAX_DIR_DEPTH = 5

# user home directory
USER_HOME = str(Path.home())

# aws config and credentials files
AWS_CONFIG_FILE = USER_HOME + '/.aws/config'
AWS_CREDENTIALS_FILE = USER_HOME + '/.aws/credentials'

AWS_SECRET_NAME = 'morphic/util/secret'

# default profile uses credentials from [DEFAULT_PROFILE] section of AWS_CREDENTIALS_FILE
# and config from [profile DEFAULT_PROFILE] section of AWS_CONFIG_FILE
DEFAULT_PROFILE = 'morphic-util'
DEFAULT_REGION = 'eu-west-2'
S3_REGION = 'us-east-1'

# local state for user
LOCAL_STATE_FILE = USER_HOME + '/.hca-util'

# Cognito and IAM
COGNITO_MORPHIC_UTIL_ADMIN = 'morphic-dev-admin'
COGNITO_CLIENT_ID = '178j951qnfuheicm2m5rqqvg6q'
COGNITO_IDENTITY_POOL_ID = 'eu-west-2:d6531e9c-020d-4ee8-bf3b-255393c500e9'
COGNITO_USER_POOL_ID = 'eu-west-2_b4EyaLNCM'
IAM_USER = 'morphic-admin'

AWS_SECRET_NAME_AK_BUCKET = 'AK-bucket'
AWS_SECRET_NAME_SK_BUCKET = 'SK-bucket'
