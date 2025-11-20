from __future__ import annotations

import os

from ait.commons.util.aws_client import Aws
from ait.commons.util.user_profile import UserProfile

from .base import Storage
from .aws_backend import AwsStorage


def build_storage(user_profile: UserProfile) -> Storage:
    """
    Build a Storage backend instance based on STORAGE_BACKEND env var.

      STORAGE_BACKEND=aws     (default) -> AwsStorage (S3)
      STORAGE_BACKEND=globus          -> GlobusStorage (on-prem via Globus)
    """
    backend = os.getenv("STORAGE_BACKEND", "aws").lower()

    if backend == "globus":
        # Lazy import so `globus_sdk` is only required in Globus mode
        from .globus_backend import GlobusStorage

        return GlobusStorage()

    # default: AWS
    return AwsStorage(Aws(user_profile))