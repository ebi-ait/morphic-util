from ait.commons.util.common import format_err
from ait.commons.util.local_state import get_selected_area


def print_area(k, area):
    print(k, end=' ')
    p = ''

    if 'perms' in area:
        p = area.get('perms') or ''
    print(p.ljust(3), end=' ')

    if 'name' in area:
        n = area.get('name')
        print(f'{n}' if n else '', end=' ')
    print()


class CmdList:
    """
    admin and user
    aws resource or client used in command - s3 resource (list_objects_v2, get_object_tagging)
    """

    def __init__(self, aws, args):
        self.aws = aws
        self.args = args

        self.s3_cli = self.aws.common_session.client('s3')

    def run(self):
        selected_area = get_selected_area()  # select area is a S3 bucket

        if not selected_area:
            return False, 'No area selected'

        try:
            self.list_bucket_contents(selected_area)
            # print_count(folder_count + files_count)
            return True, None

        except Exception as e:
            return False, format_err(e, 'list')

    def list_bucket_contents(self, selected_area, prefix=''):
        result = self.s3_cli.list_objects_v2(Bucket=selected_area, Delimiter='/', Prefix=prefix)

        # Folders
        dirs = result.get('CommonPrefixes', [])

        for d in dirs:
            k = d.get('Prefix')
            print_area(k, {'key': k, 'perms': 'dir'})
            self.list_bucket_contents(selected_area, prefix=k)

        # Files
        files = result.get('Contents', [])

        for f in files:
            k = f.get('Key')
            print_area(k, {'key': k, 'perms': 'file'})

    def list_bucket_contents_and_return(self, selected_area, prefix=''):
        """
        Lists the contents of an S3 bucket and returns a list of file keys.

        Parameters:
        - selected_area: The S3 bucket name.
        - prefix: The prefix to filter objects by (default is empty string, which lists all objects).

        Returns:
        - A list of file keys in the bucket.
        """
        file_keys = []

        def _list_bucket_contents(bucket, prefix):
            result = self.s3_cli.list_objects_v2(Bucket=bucket, Delimiter='/', Prefix=prefix)

            # Folders
            dirs = result.get('CommonPrefixes', [])
            for d in dirs:
                k = d.get('Prefix')
                # print_area(k, {'key': k, 'perms': 'dir'})
                _list_bucket_contents(bucket, prefix=k)

            # Files
            files = result.get('Contents', [])
            for f in files:
                k = f.get('Key')
                # print_area(k, {'key': k, 'perms': 'file'})
                file_keys.append(k)

        _list_bucket_contents(selected_area, prefix)
        return file_keys


def print_count(count):
    if count == 0:
        print('No item')
    elif count == 1:
        print('1 item')
    else:
        print(f'{count} items')
