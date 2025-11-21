from pathlib import Path

NAME = 'morphic-util'
VERSION = '1.0.5'
DESC = 'CLI tool for submitting your analysis data and metadata'
AUTHOR = 'dgupta'
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
# COGNITO_MORPHIC_UTIL_ADMIN = 'morphic-admin'
# COGNITO_CLIENT_ID = '6poq2i04qt3pj5rkpg51patcrk'
# COGNITO_IDENTITY_POOL_ID = 'eu-west-2:87ba188b-51fc-42e0-9172-a1a01cda8ed0'
# COGNITO_USER_POOL_ID = 'eu-west-2_2BpGQDRSU'
# IAM_USER = 'morphic-admin'

BASE_URL = 'https://api.ingest.dev.archive.morphic.bio/'

COGNITO_MORPHIC_UTIL_ADMIN = 'morphic-dev-admin'
COGNITO_CLIENT_ID = '1rfis94rvnden5elmocospd256'
COGNITO_IDENTITY_POOL_ID = 'eu-west-2:d6531e9c-020d-4ee8-bf3b-255393c500e9'
COGNITO_USER_POOL_ID = 'eu-west-2_Aqtqtg7u7'
IAM_USER = 'morphic-dev-admin'

AWS_SECRET_NAME_AK_BUCKET = 'AK-bucket'
AWS_SECRET_NAME_SK_BUCKET = 'SK-bucket'
