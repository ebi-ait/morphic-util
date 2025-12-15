from __future__ import annotations

import os
from typing import Optional

from ait.commons.util.aws_client import Aws
from ait.commons.util.user_profile import UserProfile

from .base import Storage
from .aws_backend import AwsStorage


def build_storage(user_profile: UserProfile, backend: Optional[str] = None) -> Storage:
    """
    Build a Storage backend instance.

    Precedence:
      1) explicit backend argument (e.g. 'globus' or 'aws')
      2) STORAGE_BACKEND env var
      3) default 'aws'

      backend='aws'    -> AwsStorage (S3)
      backend='globus' -> GlobusStorage (on-prem via Globus)
    """
    effective = (backend or os.getenv("STORAGE_BACKEND", "aws")).lower().strip()

    if effective == "globus":
        from .globus_backend import GlobusStorage
        return GlobusStorage()

    if effective != "aws":
        raise ValueError(f"Invalid backend '{effective}'. Expected 'aws' or 'globus'.")

    # default: AWS
    return AwsStorage(Aws(user_profile))
