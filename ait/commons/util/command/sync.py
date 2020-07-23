import os
from tqdm import tqdm
from multiprocessing import cpu_count
from multiprocessing.pool import ThreadPool

from botocore.exceptions import ClientError

from ait.commons.util.aws_client import Aws
from ait.commons.util.bucket_policy import new_policy_statement
from ait.commons.util.common import gen_uuid, format_err, INGEST_UPLOAD_AREA_PREFIX
from ait.commons.util.local_state import get_selected_area
from ait.commons.util.upload_service import notify_upload


class CmdSync:

    def __init__(self, aws: Aws, args):
        self.aws = aws
        self.args = args

    def run(self):
        if not self.aws:
            return False, 'You need configure your profile first'

        if self.aws.is_user:
            return False, 'You don\'t have permission to use this command'

        selected_area = get_selected_area()

        if not selected_area:
            return False, 'No area selected'
        
        dest_bucket, dest_env, dest_upload_area_uuid = self.args.INGEST_UPLOAD_AREA

        try:
            # Resources are not thread safe.
            # Low-level clients are thread safe. When using a low-level client, it is recommended to instantiate 
            # your client then pass that client object to each of your threads.

            s3 = self.aws.common_session.resource('s3')
            bucket = s3.Bucket(self.aws.bucket_name)

            fs = []
            total_size = 0

            # get all files from selected area
            for obj in bucket.objects.filter(Prefix=selected_area):
                # skip the top-level directory
                if obj.key == selected_area:
                    continue
                total_size += obj.size
                fs.append(obj)

            failed_fs = []
            
            def transfer(f):
                try:

                    fname = f.key[37:]
                    contentType = ''
                    obj_ = s3.meta.client.head_object(Bucket=self.aws.bucket_name, Key=f.key)
                    if obj_ and obj_['ContentType']:
                        contentType = obj_['ContentType']

                    copy_source = {
                        'Bucket': self.aws.bucket_name,
                        'Key': f.key
                    }
                    dest_key = dest_upload_area_uuid + '/' + fname

                    s3.meta.client.copy(copy_source, dest_bucket, dest_key, 
                                    Callback=pbar.update, 
                                    ExtraArgs={'ContentType': contentType})

                    if not notify_upload(dest_env, dest_upload_area_uuid, fname):
                        failed_fs.append((f, 'Transferred. Notify failed.'))

                except ClientError as ex:
                    if ex.response['Error']['Code'] == 'NoSuchKey':
                        failed_fs.append((f, 'NoSuchKey'))
                    else:
                        failed_fs.append((f, str(thread_ex)))
                    pass

                except Exception as thread_ex:
                    failed_fs.append((f, str(thread_ex)))
                    pass

            print('Transferring...')
            pbar = tqdm(total=total_size, unit='B', unit_scale=True, desc=num_files(fs))
            pool = ThreadPool(cpu_count())
            pool.map_async(transfer, fs)
            pool.close()
            pool.join()
            pbar.close()

            if failed_fs:
                print(f'{num_files(failed_fs)} failed to transfer: ')
                for f,err in failed_fs:
                    print(f'{f.key} {err}')
                return False, 'Transfer complete with error.'
            else:
                return True, 'Transfer complete.'
            
        except Exception as e:
            return False, format_err(e, 'sync')


def num_files(ls):
    l = len(ls)
    return f'{l} file{"s" if l > 1 else ""}'
