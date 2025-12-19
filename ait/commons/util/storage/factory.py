from __future__ import annotations

import os
from typing import Optional

from ait.commons.util.aws_client import Aws
from ait.commons.util.user_profile import UserProfile

from .base import Storage
from .aws_backend import AwsStorage


def build_storage(user_profile: Optional[UserProfile] = None, backend: Optional[str] = None) -> Storage:
    """
    Build a Storage backend instance.

    Precedence:
      1) explicit backend argument (e.g. 'globus' or 'aws')
      2) STORAGE_BACKEND env var
      3) default 'aws'
    """
    cfg = _load_cfg()

    effective = (
        backend
        or cfg.get("storage_backend")
        or os.getenv("STORAGE_BACKEND")
        or "aws"
    ).lower().strip()

    if effective == "globus":
        from .globus_backend import GlobusStorage
        return GlobusStorage()

    if effective != "aws":
        raise ValueError(f"Invalid backend '{effective}'. Expected 'aws' or 'globus'.")

    if user_profile is None:
        raise ValueError("AWS backend requires a user_profile, but none was provided.")

    return AwsStorage(Aws(user_profile))