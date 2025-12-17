import json
import logging
import os
import sys
from datetime import date
from pathlib import Path

import requests

from ait.commons.util.aws_client import Aws, static_bucket_name
from ait.commons.util.command.config import CmdConfig
from ait.commons.util.command.config_globus import CmdConfigGlobus
from ait.commons.util.command.create import CmdCreate
from ait.commons.util.command.delete import CmdDelete
from ait.commons.util.command.download import CmdDownload
from ait.commons.util.command.list import CmdList
from ait.commons.util.command.select import CmdSelect
from ait.commons.util.command.submit import CmdSubmit
from ait.commons.util.command.submit_file import CmdSubmitFile
from ait.commons.util.command.sync import CmdSync
from ait.commons.util.command.upload import CmdUpload
from ait.commons.util.command.view import CmdView
from ait.commons.util.local_state import get_bucket, get_attr, set_attr
from ait.commons.util.settings import NAME, VERSION
from ait.commons.util.storage.factory import build_storage
from ait.commons.util.user_profile import get_profile, profile_exists


def setup_logging(args):
    level = logging.WARNING

    if getattr(args, "debug", False):
        level = logging.DEBUG
    elif getattr(args, "verbose", False):
        level = logging.INFO

    logging.basicConfig(level=level, format="%(levelname)s: %(message)s")


def _globus_config_present() -> bool:
    """
    Detect whether the user has configured Globus by checking ~/.morphic-util/config.json.

    We avoid importing globus_backend here so AWS-only users don't pay import costs
    (or fail imports) during CLI startup.
    """
    cfg_path = Path.home() / ".morphic-util" / "config.json"
    if not cfg_path.exists():
        return False

    try:
        cfg = json.loads(cfg_path.read_text() or "{}")
    except Exception:
        return False

    # “Configured enough” to assume Globus:
    return bool(
        cfg.get("api_url")
        or cfg.get("src_collection_uuid")
        or cfg.get("refresh_token")
        or cfg.get("auth_refresh_token")
    )


def _select_backend() -> str:
    """
    Backend selection:
      - If STORAGE_BACKEND is set => respect it (aws|globus)
      - Else if ~/.morphic-util/config.json indicates Globus config => globus
      - Else => aws
    """
    env_backend = os.getenv("STORAGE_BACKEND")
    if env_backend:
        return env_backend.strip().lower()
    return "globus" if _globus_config_present() else "aws"


class Cmd:
    """
    Runner (storage-agnostic).

    Backend selection:
      - If STORAGE_BACKEND is set => respect it (aws|globus)
      - Else if ~/.morphic-util/config.json indicates Globus config => globus
      - Else => aws

    Important behavior change:
      - In Globus mode, we do NOT require a local Cognito profile (no `morphic-util config` needed).
      - In AWS mode, we DO require a Cognito profile for token + AWS credentials validation.
    """

    def __init__(self, args):
        # self.check_version()
        setup_logging(args)

        # 1) Cognito config (unchanged)
        if args.command == "config":
            success, msg = CmdConfig(args).run()
            print(msg)
            return

        # 2) Globus config – no profile/storage required
        if args.command == "config-globus":
            success, msg = CmdConfigGlobus(args).run()
            if msg:
                print(msg)
            return

        # 3) These bypass storage/profile
        if args.command == "submit":
            success, msg = CmdSubmit(args).run()
            if msg:
                print(msg)
            return

        if args.command == "submit-file":
            success, msg = CmdSubmitFile(args).run()
            if msg:
                print(msg)
            return

        if args.command == "view":
            success, msg = CmdView(args).run()
            if msg:
                print(msg)
            return

        # -------- Backend selection (MUST be before profile checks) --------
        backend = _select_backend()

        if backend not in ("aws", "globus"):
            print(f"Invalid STORAGE_BACKEND='{backend}'. Expected 'aws' or 'globus'.")
            sys.exit(1)

        print(f"[morphic-util] storage backend: {backend}")

        if backend == "globus":
            # Globus mode: do NOT require a Cognito profile.
            # build_storage should ignore user_profile (or accept None) in globus mode.
            self.user_profile = None
            self.storage = build_storage(None, backend="globus")

        else:
            # AWS mode: require a user profile + Cognito token + AWS credentials + bucket bootstrap.
            if not profile_exists(args.profile):
                print(
                    f"Profile '{args.profile}' not found. "
                    f"Run: morphic-util config <username> <password>"
                )
                sys.exit(1)

            self.user_profile = get_profile(args.profile)

            access_token = getattr(self.user_profile, "access_token", None)
            if not access_token:
                print("Not authenticated. Run: morphic-util config <username> <password>")
                sys.exit(1)

            self.aws = Aws(self.user_profile)
            if not self.aws.is_valid_credentials():
                print("Invalid credentials")
                sys.exit(1)

            bucket = get_bucket()
            if bucket:
                self.aws.bucket_name = bucket
            else:
                try:
                    static_bucket_name()
                except Exception:
                    print("Unable to get bucket")
                    sys.exit(1)

            self.storage = build_storage(self.user_profile, backend="aws")

        self.execute(args)

    def check_version(self):
        today = date.today()
        last_checked = get_attr("version_checked")

        if not last_checked or last_checked < today:
            resp = requests.get(f"https://pypi.org/pypi/{NAME}/json")
            latest_version = resp.json()["info"]["version"]
            if VERSION < latest_version:
                print(
                    f"INFO: A new version of {NAME} is available. "
                    f"Run `pip install {NAME} --upgrade` to upgrade."
                )
            set_attr("version_checked", today)

    def execute(self, args):
        if args.command == "create":
            success, msg = CmdCreate(self.storage, args).run()
            self.exit(success, msg)

        elif args.command == "select":
            success, msg = CmdSelect(self.storage, args).run()
            self.exit(success, msg)

        elif args.command == "list":
            success, msg = CmdList(self.storage, args).run()
            self.exit(success, msg)

        elif args.command == "upload":
            success, msg = CmdUpload(self.storage, args).run()
            self.exit(success, msg)

        elif args.command == "download":
            success, msg = CmdDownload(self.storage, args).run()
            self.exit(success, msg)

        elif args.command == "delete":
            success, msg = CmdDelete(self.storage, args).run()
            self.exit(success, msg)

        elif args.command == "sync":
            success, msg = CmdSync(self.storage, args).run()
            self.exit(success, msg)

        else:
            print(f"Unknown command: {args.command}")
            sys.exit(1)

    def exit(self, success, message):
        if message:
            print(message)
        sys.exit(0 if success else 1)
