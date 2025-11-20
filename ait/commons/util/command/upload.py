import hashlib, os, filetype, concurrent.futures
from concurrent.futures import ThreadPoolExecutor
from ait.commons.util.settings import DIR_SUPPORT, MAX_DIR_DEPTH
from ait.commons.util.common import format_err
from ait.commons.util.local_state import get_selected_area
from ait.commons.util.progress_bar import ProgressBar

def compute_md5(file_path):
    h = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""): h.update(chunk)
    return h.hexdigest()

class CmdUpload:
    def __init__(self, storage, args):
        self.storage = storage
        self.args = args

    def upload_file(self, selected_area, data_file, destination_file, file_index, total_files):
        # NOTE: Arguments must match the new definition in GlobusStorage.upload_file
        md5_hex = compute_md5(data_file)
        print(f"MD5 hash of {data_file} is {md5_hex}")

        overwrite = getattr(self.args, 'o', False)
        file_size = os.path.getsize(data_file)
        if not overwrite and self.storage.data_file_exists(selected_area, destination_file):
            print(f"{destination_file} already exists. Use -o to overwrite."); return
        if file_size == 0:
            print(f"{data_file} is an empty file"); return

        ft = filetype.guess(data_file)
        content_type = (ft.mime if ft else 'application/octet-stream') + '; dcp-type=data'
        progress = ProgressBar(target=data_file, total=file_size)

        # Passing new arguments to the storage layer
        self.storage.upload_file(selected_area, data_file, destination_file,
                                 file_index=file_index, total_files=total_files, # <-- NEW
                                 content_type=content_type, md5_hex=md5_hex,
                                 overwrite=overwrite, progress_cb=progress)


    def upload_files(self, data_files, prefix):
        selected_area = prefix

        # Initialize counters
        total_files = len(data_files)
        successful_uploads = 0 # <-- New counter for success

        indexed_tasks = [
            (i + 1, f, os.path.basename(f))
            for i, f in enumerate(data_files)
        ]

        with ThreadPoolExecutor() as executor:
            futures = {}

            # Submit tasks sequentially (to ensure ordered [N/M] Submitting messages)
            for idx, f, dest_f in indexed_tasks:
                # Print submission message in sequential order

                future = executor.submit(
                    self.upload_file,
                    selected_area,
                    f,
                    dest_f,
                    idx,
                    total_files
                )
                futures[future] = f

            # Process results as they complete (concurrently)
            for fut in concurrent.futures.as_completed(futures):
                file_path = futures[fut]
                try:
                    # If result() completes without exception, the file was successful
                    fut.result()
                    successful_uploads += 1 # <-- Increment success counter
                except Exception as ex:
                    print(f"Exception raised for {file_path}: ", ex)

        # 🌟 Final Summary Print 🌟
        # Print this after the progress bars and exceptions have finished.
        print(f"\n--- Upload Summary ---")
        print(f"Successfully uploaded: {successful_uploads} out of {total_files} files.")
        print("----------------------")

        return successful_uploads == total_files # Return True only if all files succeeded

    def run(self):
        selected_area = get_selected_area()
        if not selected_area: return False, 'No area selected'
        try:
            # collect targets
            paths = []
            for p in self.args.PATH:
                p = os.path.abspath(p)
                if p not in paths: paths.append(p)

            files = []
            max_depth = MAX_DIR_DEPTH if (DIR_SUPPORT and getattr(self.args,'r',False)) else 1
            def walk(curr, level):
                if level < max_depth:
                    level += 1
                    for name in os.listdir(curr):
                        if name.startswith('.') or name.startswith('__'): continue
                        full = os.path.join(curr, name)
                        if os.path.isfile(full): files.append(full)
                        elif os.path.isdir(full): walk(full, level)

            for p in paths:
                if os.path.isfile(p): files.append(p)
                elif os.path.isdir(p): walk(p, 0)

            print('Uploading...')
            ok = self.upload_files(files, selected_area)
            return (ok, "Successful upload") if ok else (ok, "Failed upload")
        except Exception as e:
            return False, format_err(e, 'upload')