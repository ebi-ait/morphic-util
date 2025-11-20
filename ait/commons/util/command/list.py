# ait/commons/util/command/list.py

import os
import csv
from typing import List

from ait.commons.util.common import format_err
from ait.commons.util.local_state import get_selected_area
from ait.commons.util.user_profile import get_profile


def print_area(name: str, *, perms: str = "", md5: str | None = None):
    """
    Match the original print format:
      <name> <perms.ljust(3)> <md5.ljust(3)>
    """
    perms_str = (perms or "").ljust(3)
    md5_str = (md5 or "").ljust(3) if md5 is not None else "".ljust(3)
    print(f"{name} {perms_str} {md5_str}")


class CmdList:
    """
    Backend-agnostic list command.

    Expects a Storage backend (AwsStorage or GlobusStorage) to be passed in:
      - storage.list(dataset, prefix) -> List[str]
      - For on-prem (GlobusStorage), list() returns flat file names at dataset root.
      - For AWS (AwsStorage), implement list() to return a flat list of keys too.

    --processing (admin-only):
      Writes a TSV of files. Since MD5 object metadata is S3-specific and on-prem
      uses filesystem, we provide a cross-backend hint column indicating whether
      a '<file>.md5' sidecar exists. (If you want real MD5 values on-prem, have
      the Provider API persist and expose them; then extend the Storage interface.)
    """

    def __init__(self, storage, args):
        self.storage = storage
        self.args = args
        self.user = get_profile('morphic-util').username
        self.processing = getattr(self.args, 'processing', None)
        self._backend = os.getenv("STORAGE_BACKEND", "aws").lower()

    def run(self):
        selected_area = get_selected_area()
        if not selected_area:
            return False, 'No area selected'

        try:
            files = self.storage.list(selected_area, "")

            # If the backend is globus, we assume the list() method returns
            # pre-formatted lines (including headers and size), so we just print them.
            if self._backend == "globus":
                for line in files:
                    print(line)

                # Handle the optional admin TSV output for the globus case
                if self.processing:
                    if self.user != 'morphic-admin':
                        return False, "Admin function only"
                    print("\nWARNING: TSV output skipped for 'globus' backend as raw file names are unavailable.")

                return True, None

            # The logic below is for the AWS backend or when list() returns raw file names.

            # Build a quick lookup for ".md5" sidecars so we can surface a hint
            md5_sidecars = {f[:-4] for f in files if f.endswith(".md5")}

            # Print items — directories are not returned by v1 Globus list();
            # if AwsStorage chooses to include folder markers (ending with '/'),
            # we'll show them as 'dir'.
            for f in sorted(files):
                if f.endswith("/"):
                    print_area(f, perms="dir", md5=None)
                else:
                    md5_hint = "md5" if f in md5_sidecars else None
                    print_area(f, perms="file", md5=md5_hint)

            # Optional admin TSV output
            if self.processing:
                if self.user != 'morphic-admin':
                    return False, "Admin function only"
                self._write_tsv(selected_area, files, md5_sidecars)

            return True, None

        except Exception as e:
            return False, format_err(e, 'list')

    # Kept for compatibility with submit-file:
    # submit-file calls list_bucket_contents_and_return(self.dataset, '')
    def list_bucket_contents_and_return(self, selected_area, prefix='') -> List[str]:
        return self.storage.list(selected_area, prefix or "")

    # ---------------- internal helpers ----------------

    def _write_tsv(self, selected_area: str, files: List[str], md5_sidecars: set):
        """
        Cross-backend TSV writer. Columns:
          - File Name
          - MD5 Sidecar (yes|no)
        Note: If you need actual MD5 values:
          - AWS: extend AwsStorage.list() to include metadata per key, or add a new API.
          - On-prem: have Provider API persist MD5 on upload and expose it in listing.
        """
        output_file = 'file_md5s.tsv'
        with open(output_file, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile, delimiter=',')
            writer.writerow(['File Name', 'MD5 Sidecar'])
            for f in sorted(files):
                if f.endswith("/"):
                    # skip directory markers in TSV
                    continue
                writer.writerow([f, 'yes' if f in md5_sidecars else 'no'])

        print(f"\nResults saved to {output_file}")