import json
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
from ait.commons.util.local_state import get_bucket, set_attr, get_attr
from ait.commons.util.settings import NAME, VERSION
from ait.commons.util.user_profile import profile_exists, get_profile

# storage factory (returns AwsStorage or GlobusStorage based on env / config)
from ait.commons.util.storage.factory import build_storage


def _globus_config_present() -> bool:
    """
    Detect whether the user has configured Globus by checking ~/.morphic-util/config.json.

    IMPORTANT:
    - Don't treat 'api_url' as a signal, because defaults may populate it.
    - Only treat Globus as configured if there's real Globus config/auth present.
    """
    cfg_path = Path.home() / ".morphic-util" / "config.json"
    if not cfg_path.exists():
        return False

    try:
        cfg = json.loads(cfg_path.read_text() or "{}")
    except Exception:
        return False

    return bool(
        cfg.get("src_collection_uuid")
        or cfg.get("refresh_token")
        or cfg.get("auth_refresh_token")
    )


class Cmd:
    """
    Runner (storage-agnostic).

    - Auth: Always requires AWS Cognito (access token in the user profile),
            regardless of backend.
    - Backend selection:
        * If STORAGE_BACKEND is set => use it (aws|globus)
        * Else if ~/.morphic-util/config.json indicates Globus config => globus
        * Else => aws
    """

    def __init__(self, args):
        # self.check_version()

        # 1) Cognito config (unchanged)
        if args.command == "config":
            success, msg = CmdConfig(args).run()
            if msg:
                print(msg)
            return

        # 2) Globus config – no AWS profile required; writes ~/.morphic-util/config.json
        if args.command == "config-globus":
            success, msg = CmdConfigGlobus(args).run()
            if msg:
                print(msg)
            return

        # 3) These also bypass storage/profile (as per current design)
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

        # ---- from here on, we require a user profile + Cognito token ----

        if not profile_exists(args.profile):
            print(
                f"Profile '{args.profile}' not found. Please run config command with your access keys",
                file=sys.stderr,
            )
            sys.exit(1)

        self.user_profile = get_profile(args.profile)

        access_token = getattr(self.user_profile, "access_token", None)
        if not access_token:
            print("Not authenticated. Run: morphic-util config <username> <password>", file=sys.stderr)
            sys.exit(1)

        # -------- Backend selection --------
        env_backend = os.getenv("STORAGE_BACKEND")
        if env_backend:
            backend = env_backend.strip().lower()
        else:
            backend = "globus" if _globus_config_present() else "aws"

        if backend not in ("aws", "globus"):
            print(f"Invalid STORAGE_BACKEND='{backend}'. Expected 'aws' or 'globus'.", file=sys.stderr)
            sys.exit(1)

        print(f"[morphic-util] storage backend: {backend}", file=sys.stderr)

        if backend == "aws":
            self.aws = Aws(self.user_profile)

            if not self.aws.is_valid_credentials():
                print("Invalid credentials", file=sys.stderr)
                sys.exit(1)

            bucket = get_bucket()
            if bucket:
                self.aws.bucket_name = bucket
            else:
                try:
                    static_bucket_name()
                except Exception:
                    print("Unable to get bucket", file=sys.stderr)
                    sys.exit(1)

            # storage from factory (AwsStorage)
            self.storage = build_storage(self.user_profile)

        else:
            # globus backend: no S3 bucket, just storage from factory (GlobusStorage)
            self.storage = build_storage(self.user_profile)

        self.execute(args)

    def check_version(self):
        today = date.today()
        last_checked = get_attr("version_checked")

        if not last_checked or last_checked < today:
            resp = requests.get(f"https://pypi.org/pypi/{NAME}/json", timeout=10)
            latest_version = resp.json()["info"]["version"]
            if VERSION < latest_version:
                print(f"INFO: A new version of {NAME} is available. Run `pip install {NAME} --upgrade` to upgrade.")
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
            print(f"Unknown command: {args.command}", file=sys.stderr)
            sys.exit(1)

    def exit(self, success, message):
        if message:
            print(message)
        sys.exit(0 if success else 1)
