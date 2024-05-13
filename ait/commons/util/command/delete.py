from ait.commons.util.common import format_err
from ait.commons.util.local_state import get_selected_area

'''
ToDo
1. fix delete object
if an object key is specified e.g. abc.txt, current behaviour is deleting all objects with prefix 'abc.txt' for e.g. 'abc.txt2'

'''


class CmdDelete:
    """
    both admin and user, though user can't delete folder
    aws resource or client used in command - s3 resource (bucket.objects/ obj.delete)
    """

    def __init__(self, aws, args):
        self.aws = aws
        self.args = args

    def run(self):

        selected_area = get_selected_area()

        if not selected_area:
            return False, 'No area selected'

        try:
            if self.args.a:  # delete all files

                confirm = input(f'Confirm delete all contents from {selected_area}? Y/y to proceed: ')

                if confirm.lower() == 'y':
                    print('Deleting...')

                    deleted_keys = self.delete_all_files_from_s3_bucket(selected_area, incl_selected_area=False)

                    for k in deleted_keys:
                        print(k)

                return True, None

            if self.args.PATH:  # list of files and dirs to delete
                print('Deleting...')

                for p in self.args.PATH:
                    # you may have perm x but not d (to load or even do a head object)
                    # so use obj_exists

                    prefix = p
                    keys = self.all_keys(selected_area, prefix)

                    if keys:
                        for k in keys:
                            try:
                                self.delete_singe_file_from_s3_bucket(selected_area, k)
                                print(k + '  Done.')
                            except Exception as ex:
                                if 'AccessDenied' in str(ex):
                                    print('No permission to delete.')
                                else:
                                    print('Delete failed.')
                    else:
                        print(prefix + '  File not found.')
                return True, None
            else:
                return False, 'No path specified'

        except Exception as e:
            return False, format_err(e, 'delete')

    # based on obj_exists method
    def all_keys(self, selected_area, prefix):
        keys = []
        response = self.aws.common_session.client('s3').list_objects_v2(
            Bucket=selected_area,
            Prefix=prefix,
        )
        for obj in response.get('Contents', []):
            keys.append(obj['Key'])

        return keys

    def delete_singe_file_from_s3_bucket(self, selected_area, key):
        s3_resource = self.aws.common_session.resource('s3')
        s3_obj = s3_resource.ObjectSummary(selected_area, key)
        s3_obj.delete()
        return key

    def delete_all_files_from_s3_bucket(self, selected_area, incl_selected_area=False):
        s3_resource = self.aws.common_session.resource('s3')
        bucket = s3_resource.Bucket(selected_area)
        deleted_keys = []
        objs_to_delete = bucket.objects.filter() if incl_selected_area else filter(
            lambda obj: obj.key != selected_area, bucket.objects.filter())
        for obj in objs_to_delete:
            obj.delete()
            deleted_keys.append(obj.key)

        return deleted_keys
