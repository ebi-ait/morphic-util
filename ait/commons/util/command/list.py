import hashlib
import csv

from ait.commons.util.common import format_err
from ait.commons.util.local_state import get_selected_area
from ait.commons.util.user_profile import get_profile
from urllib.parse import urlparse


def print_area(k, area):
    print(k, end=' ')
    p = ''

    if 'perms' in area:
        p = area.get('perms') or ''
    print(p.ljust(3), end=' ')

    if 'md5' in area:
        p = area.get('md5') or ''
    print(p.ljust(3), end=' ')

    if 'name' in area:
        n = area.get('name')
        print(f'{n}' if n else '', end=' ')
    print()


def get_s3_path():
    while True:
        s3_path = input("Enter the S3 path (e.g., s3://bucket-name/folder/): ").strip()
        parsed_url = urlparse(s3_path)

        if parsed_url.scheme == 's3' and parsed_url.netloc:
            return s3_path
        else:
            print("Invalid S3 path. Please enter a valid S3 path starting with 's3://'.")


def calculate_md5(s3_client, bucket_name, key):
    md5_hash = hashlib.md5()

    try:
        # Stream the object in chunks
        response = s3_client.get_object(Bucket=bucket_name, Key=key)

        for chunk in response['Body'].iter_chunks(chunk_size=8192):
            md5_hash.update(chunk)

        return md5_hash.hexdigest()
    except Exception as e:
        print(f"Failed to compute MD5 for {key}: {e}")

        return None


class CmdList:
    """
    admin and user
    aws resource or client used in command - s3 resource (list_objects_v2, get_object_tagging)
    """

    def __init__(self, aws, args):
        self.aws = aws
        self.args = args
        self.user = get_profile('morphic-util').username
        self.processing = getattr(self.args, 'processing', None)

        self.s3_cli = self.aws.common_session.client('s3')

    def run(self):
        if self.processing:
            if self.user != 'morphic-admin':
                return False, "Admin function only"
            else:
                print("Access granted")

                s3_path = get_s3_path()
                self.list_s3_files(s3_path)

                return True, None

        else:
            selected_area = get_selected_area()  # select area is a S3 bucket

            if not selected_area:
                return False, 'No area selected'

            try:
                self.list_bucket_contents(selected_area)
                # print_count(folder_count + files_count)
                return True, None

            except Exception as e:
                return False, format_err(e, 'list')

    def list_s3_files(self, s3_path):
        parsed_url = urlparse(s3_path)
        bucket_name = parsed_url.netloc
        prefix = parsed_url.path.lstrip('/')
        output_file = 's3_file_md5s.tsv'

        with open(output_file, 'w', newline='') as csvfile:
            tsv_writer = csv.writer(csvfile, delimiter=',')
            tsv_writer.writerow(['File Name', 'MD5 Hash'])  # Write header row

            try:
                response = self.s3_cli.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

                if 'Contents' in response:
                    print(f"\nFiles in '{s3_path}'")

                    for obj in response['Contents']:
                        file_key = obj['Key']
                        if not file_key.endswith('/'):  # Skip folders
                            md5_hash = calculate_md5(self.s3_cli, bucket_name, file_key)

                            if md5_hash:
                                print(f"{file_key} - MD5: {md5_hash}")
                                tsv_writer.writerow([file_key, md5_hash])  # Write to file
                else:
                    print("\nNo files found.")
            except Exception as e:
                print(f"\nError: {e}")

        print(f"\nResults saved to {output_file}")

    def list_bucket_contents(self, selected_area, prefix=''):
        result = self.s3_cli.list_objects_v2(Bucket=selected_area, Delimiter='/', Prefix=prefix)
        # Folders
        dirs = result.get('CommonPrefixes', [])

        for d in dirs:
            k = d.get('Prefix')
            print_area(k, {'key': k, 'md5': None, 'perms': 'dir'})
            self.list_bucket_contents(selected_area, prefix=k)

        # Files
        files = result.get('Contents', [])

        for f in files:
            k = f.get('Key')
            head_object_response = self.s3_cli.head_object(Bucket=selected_area, Key=k)
            metadata = head_object_response.get('Metadata', {})
            hash_md5 = metadata.get('md5', 'MD5 checksum not found')
            print_area(k, {'key': k, 'md5': hash_md5, 'perms': 'file'})

    def list_bucket_contents_and_return(self, selected_area, prefix=''):
        """
        Lists the contents of an S3 bucket and returns a list of file keys.

        Parameters:
        - selected_area: The S3 bucket name.
        - prefix: The prefix to filter objects by (default is empty string, which lists all objects).

        Returns:
        - A list of file keys in the bucket.
        """
        file_keys = []  # Initialize an empty list to store file keys.

        # Define the recursive function to list bucket contents.
        def _list_bucket_contents(bucket, prefix):
            # Call AWS S3 API to list objects with a specific prefix.
            result = self.s3_cli.list_objects_v2(Bucket=bucket, Delimiter='/', Prefix=prefix)

            # Handle directories (folders) first.
            dirs = result.get('CommonPrefixes', [])
            for d in dirs:
                k = d.get('Prefix')
                # Recursively call the function to list contents of the subdirectory.
                _list_bucket_contents(bucket, prefix=k)

            # Handle files at the current prefix level.
            files = result.get('Contents', [])
            for f in files:
                k = f.get('Key')
                # Add each file key to the list.
                file_keys.append(k)

        # Start the recursive process to list all contents from the given prefix.
        _list_bucket_contents(selected_area, prefix)

        # Return the final list of all file keys found in the bucket.
        return file_keys


def print_count(count):
    if count == 0:
        print('No item')
    elif count == 1:
        print('1 item')
    else:
        print(f'{count} items')
