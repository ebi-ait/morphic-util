from ait.commons.util.aws_cognito_authenticator import AwsCognitoAuthenticator
from ait.commons.util.common import format_err
from ait.commons.util.local_state import set_bucket
from ait.commons.util.settings import DEFAULT_PROFILE


class CmdConfig:
    """
    both admin and user
    aws resource or client used in command - sts (to check valid credentials).
    """

    def __init__(self, args):
        self.args = args

    def run(self):

        global valid_user

        try:
            profile = self.args.profile if self.args.profile else DEFAULT_PROFILE
            aws_cognito_authenticator = AwsCognitoAuthenticator(self)

            # TODO: review the below bucket in args
            if self.args.bucket:
                set_bucket(self.args.bucket)

            if self.args.USERNAME and self.args.PASSWORD:
                valid_user = aws_cognito_authenticator.is_registered_user(profile, self.args.USERNAME,
                                                                          self.args.PASSWORD)
            else:
                print("No credentials provided!")

            # check if valid user
            if valid_user:
                return True, 'Valid credentials'
            else:
                return False, 'Invalid credentials'
        except Exception as e:
            return False, format_err(e, 'config')
