from __future__ import annotations

from typing import List

from ait.commons.util.aws_client import Aws
from .base import Storage, ProgressCb


class AwsStorage(Storage):
    """
    S3-based implementation of Storage, using the existing Aws helper.

    In the current model, each 'area' is an S3 bucket whose name is the DATASET_ID.
    """

    def __init__(self, aws: Aws):
        self.aws = aws

    # --- area-level ---

    def area_exists(self, area: str) -> bool:
        return self.aws.s3_bucket_exists(area)

    # --- file-level ---

    def data_file_exists(self, area: str, dest_name: str) -> bool:
        # Original behaviour: use Aws helper
        return self.aws.data_file_exists(area, dest_name)

    def upload_file(
        self,
        area: str,
        local_path: str,
        dest_name: str,
        *,
        content_type: str | None,
        md5_hex: str | None,
        overwrite: bool = False,
        progress_cb: ProgressCb = None,
    ) -> None:
        """
        Upload to S3.

        NOTE: This preserves original behaviour: file bytes are streamed
        directly to S3 with optional MD5 metadata.
        """
        session = self.aws.new_session()
        s3 = session.resource("s3")

        extra = {
            "ContentType": content_type or "application/octet-stream",
            "Metadata": {"md5": md5_hex or ""},
        }

        s3.Bucket(area).upload_file(
            Filename=local_path,
            Key=dest_name,
            Callback=progress_cb,
            ExtraArgs=extra,
        )

    def list(self, area: str, prefix: str = "") -> List[str]:
        """
        Return a flat list of keys.

        This behaves like the previous list_bucket_contents_and_return().
        """
        return self.aws.list_bucket_contents_and_return(area, prefix)
