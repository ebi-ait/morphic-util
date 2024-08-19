import json

import boto3
import botocore

from ait.commons.util.aws_cognito_authenticator import AwsCognitoAuthenticator
from ait.commons.util.settings import AWS_SECRET_NAME_AK_BUCKET, AWS_SECRET_NAME_SK_BUCKET, \
    COGNITO_MORPHIC_UTIL_ADMIN, S3_REGION


def static_bucket_name():
    return 'morphic-bio'


class Aws:

    def __init__(self, user_profile):
        self.is_user = True  # not admin
        self.user_dir_list = None
        self.center_name = None
        self.secret_key = None
        self.access_key = None
        self.user_profile = user_profile
        self.common_session = self.new_session()
        self.bucket_name = 'morphic-bio'

    def get_access_key(self, secret_mgr_client):
        resp = secret_mgr_client.get_secret_value(SecretId=AWS_SECRET_NAME_AK_BUCKET)
        secret_str = resp['SecretString']
        self.access_key = json.loads(secret_str)['AK-bucket']
        return self.access_key

    def get_secret_key(self, secret_mgr_client):
        resp = secret_mgr_client.get_secret_value(SecretId=AWS_SECRET_NAME_SK_BUCKET)
        secret_str = resp['SecretString']
        self.secret_key = json.loads(secret_str)['SK-bucket']
        return self.secret_key

    def get_bucket_name(self, secret_mgr_client):
        """
        Get bucket name from aws secrets
        :return:
        """
        # access policy can't be attached to a secret
        # GetSecretValue action should be allowed for user
        resp = secret_mgr_client.get_secret_value(SecretId='')
        secret_str = resp['SecretString']
        self.bucket_name = json.loads(secret_str)['s3-bucket']
        return self.bucket_name

    def new_session(self):
        aws_cognito_authenticator = AwsCognitoAuthenticator(self)
        secret_manager_client = aws_cognito_authenticator.secret_manager_client_instance(self.user_profile.username,
                                                                                         self.user_profile.password)

        if secret_manager_client is None:
            print(
                'Failure while re-establishing Amazon Web Services session, report this error to the MorPhiC DRACC '
                'admin')
            raise Exception
        else:
            self.is_user = aws_cognito_authenticator.is_user
            self.user_dir_list = aws_cognito_authenticator.get_user_dir_list()
            self.center_name = aws_cognito_authenticator.get_center_name()

            return boto3.Session(region_name=S3_REGION,
                                 aws_access_key_id=self.get_access_key(secret_manager_client),
                                 aws_secret_access_key=self.get_secret_key(secret_manager_client))

    def is_valid_credentials(self):
        """
        Validate user config/credentials by making a get_caller_identity aws api call
        :return:
        """
        sts = self.common_session.client('sts')

        try:
            resp = sts.get_caller_identity()
            arn = resp.get('Arn')
            if arn.endswith(COGNITO_MORPHIC_UTIL_ADMIN):
                return True
        except Exception as e:
            if e is not KeyboardInterrupt:
                return False
            else:
                raise e

    def is_valid_user(self):
        return self.is_user

    def s3_bucket_exists(self, key):
        """
        Returns True if the bucket exists, else False.
        """
        client = self.common_session.client('s3')
        try:
            client.head_bucket(
                Bucket=key
            )
            return True
        except client.exceptions.NoSuchBucket as e:
            print(f"The bucket '{key}' does not exist. Reason: {e}")
            return False

    def data_file_exists(self, bucket_name, key):
        """
        Check if an object exists in the specified S3 bucket.

        Parameters:
        - bucket_name (str): The name of the S3 bucket.
        - key (str): The key of the object in the bucket.

        Returns:
        - bool: True if the object exists, False otherwise.
        """
        client = self.common_session.client('s3')

        try:
            client.head_object(Bucket=bucket_name, Key=key)
            return True
        except client.exceptions.ClientError:
            return False
