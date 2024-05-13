from ait.commons.util.aws_client import Aws


# TODO: review
def run():
    return False, ('create is no longer supported as upload areas (buckets) '
                   'are created while metadata submission')


class CmdCreate:
    """
    admin only
    aws resource or client used in command - s3 client (put_object), s3 resource (BucketPolicy)
    """

    def __init__(self, aws: Aws, args):
        self.aws = aws
        self.args = args
