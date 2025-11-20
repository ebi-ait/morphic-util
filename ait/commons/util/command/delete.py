from ait.commons.util.common import format_err
from ait.commons.util.local_state import get_selected_area
from typing import List
from ait.commons.util.storage.globus_backend import GlobusStorage

class CmdDelete:
    """
    AWS S3 and Globus-backed deletion commands.
    """

    def __init__(self, aws, args):
        self.aws = aws
        self.args = args
        self.globus_storage: Optional[GlobusStorage] = None

        # Check if Globus mode is requested (assuming -g is the flag)
        if hasattr(self.args, 'g') and self.args.g:
            # Instantiate the existing GlobusStorage class
            self.globus_storage = GlobusStorage()

    def run(self):

        selected_area = get_selected_area()

        if not selected_area:
            return False, 'No area selected'

        # --- Delegate to Globus Mode if requested ---
        if self.globus_storage:
            return self._run_globus_delete(selected_area)

        # --- Default to S3 Mode ---
        return self._run_s3_delete(selected_area)

    # -----------------------------------------------------------------
    # Globus Deletion Logic (Delegates to the existing GlobusStorage.delete)
    # -----------------------------------------------------------------
    def _run_globus_delete(self, selected_area):
        try:
            # Scenario 1: Delete all contents (-a)
            if self.args.a:
                confirm = input(f'Confirm delete all contents from Globus area {selected_area}? Y/y to proceed: ')
                if confirm.lower() == 'y':
                    print('Submitting Globus deletion task...')
                    # Call the existing GlobusStorage.delete method
                    self.globus_storage.delete(selected_area, all_contents=True)
                return True, None

            # Scenario 2: Delete specific paths
            if self.args.PATH:
                print('Submitting Globus deletion task...')
                # Call the existing GlobusStorage.delete method
                self.globus_storage.delete(selected_area, paths=self.args.PATH)
                # Note: Globus delete is asynchronous (task submitted)
                return True, None

            return False, 'No path specified for Globus delete'

        except RuntimeError as e:
            # Catch exceptions raised by _api_call within GlobusStorage.delete
            return False, format_err(e, 'Globus API delete')
        except Exception as e:
            return False, format_err(e, 'Globus delete setup')

    # -----------------------------------------------------------------
    # S3 Deletion Logic (Adapted from previous response)
    # -----------------------------------------------------------------
    def _run_s3_delete(self, selected_area):
        try:
            if self.args.a:  # delete all files
                confirm = input(f'Confirm delete all contents from S3 area {selected_area}? Y/y to proceed: ')
                if confirm.lower() == 'y':
                    print('Deleting...')
                    deleted_keys = self._delete_all_files_from_s3_bucket(selected_area, incl_selected_area=False)
                    if not deleted_keys:
                        print('No files found to delete.')
                    else:
                        for k in deleted_keys:
                            print(k)
                return True, None

            if self.args.PATH:  # list of files and dirs to delete
                print('Deleting...')
                for p in self.args.PATH:
                    key_to_delete = self._check_and_get_key_for_delete_s3(selected_area, p)
                    if key_to_delete:
                        try:
                            self._delete_single_file_from_s3_bucket(selected_area, key_to_delete)
                            print(key_to_delete + '  Done.')
                        except Exception as ex:
                            if 'AccessDenied' in str(ex):
                                print(key_to_delete + '  No permission to delete.')
                            else:
                                print(f'{key_to_delete}  Delete failed: {str(ex)}')
                    else:
                        print(p + '  File not found.')
                return True, None
            else:
                return False, 'No path specified'
        except Exception as e:
            return False, format_err(e, 'S3 delete')

    # --- S3 Helper Methods ---
    def _check_and_get_key_for_delete_s3(self, selected_area: str, key_or_prefix: str) -> str | None:
        """Helper to ensure exact S3 key match, preventing prefix deletion."""
        s3_client = self.aws.common_session.client('s3')
        response = s3_client.list_objects_v2(
            Bucket=selected_area,
            Prefix=key_or_prefix,
            MaxKeys=2
        )
        contents: List[dict] = response.get('Contents', [])

        if len(contents) >= 1 and any(obj['Key'] == key_or_prefix for obj in contents):
            return key_or_prefix

        return None

    def _delete_single_file_from_s3_bucket(self, selected_area: str, key: str):
        """Deletes a single object by its exact key."""
        s3_resource = self.aws.common_session.resource('s3')
        s3_obj = s3_resource.ObjectSummary(selected_area, key)
        s3_obj.delete()
        return key

    def _delete_all_files_from_s3_bucket(self, selected_area: str, incl_selected_area: bool = False) -> List[str]:
        """Deletes all objects in the bucket using a batch delete operation."""
        s3_resource = self.aws.common_session.resource('s3')
        bucket = s3_resource.Bucket(selected_area)
        deleted_keys = []

        objs_to_delete = bucket.objects.filter()

        keys_to_delete = []
        for obj in objs_to_delete:
            if incl_selected_area or obj.key != selected_area:
                keys_to_delete.append({'Key': obj.key})

        if keys_to_delete:
            response = bucket.delete_objects(
                Delete={'Objects': keys_to_delete, 'Quiet': True}
            )
            if 'Deleted' in response:
                deleted_keys.extend(d['Key'] for d in response['Deleted'])

        return deleted_keys