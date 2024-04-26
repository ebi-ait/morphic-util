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
        selected_area = get_selected_area()

        if not selected_area:
            return False, 'No area selected'

        try:
            # selected_area += '' if selected_area.endswith('/') else '/'
            folder_count = 0
            for area in self.list_bucket_areas(selected_area):
                k = area["key"]
                print_area(k, area)
                folder_count += 1
            print_count(folder_count)
            return True, None

        except Exception as e:
            return False, format_err(e, 'list')

    def list_bucket_areas(self, selected_area):
        areas = []
        result = self.s3_cli.list_objects_v2(Bucket=selected_area, Delimiter='/')

        # Folders
        dirs = result.get('CommonPrefixes', [])
        for d in dirs:
            k = d.get('Prefix')
            areas.append({'key': k})

        # Files
        files = result.get('Contents', [])
        for f in files:
            k = f.get('Key')
            areas.append({'key': k})

        return areas


def print_count(count):
    if count == 0:
        print('No item')
    elif count == 1:
        print('1 item')
    else:
        print(f'{count} items')
