import os
import sys
from datetime import date

import requests

from ait.commons.util.aws_client import Aws, static_bucket_name

from ait.commons.util.command.config import CmdConfig
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
from ait.commons.util.command.config_globus import CmdConfigGlobus

# NEW: storage factory (returns AwsStorage or GlobusStorage based on env)
from ait.commons.util.storage.factory import build_storage


class Cmd:
    """
    Runner (storage-agnostic).

    - Auth: Always requires AWS Cognito (access token in the user profile),
            regardless of backend.
    - Backend selection: via env var STORAGE_BACKEND=aws|globus
        * aws    -> Do legacy AWS STS checks & bucket bootstrap.
        * globus -> Skip STS & bucket; use Provider API + Globus on-prem storage.
    """

    def __init__(self, args):

        # self.check_version()

        # 1) Cognito config (unchanged)
        if args.command == 'config':
            success, msg = CmdConfig(args).run()
            print(msg)
            return

        # 2) NEW: Globus config – no profile, no storage, just Globus NativeApp flow
        if args.command == 'config-globus':
            success, msg = CmdConfigGlobus(args).run()
            if msg:
                print(msg)
            return

        # 3) These also bypass storage/profile
        if args.command == 'submit':
            success, msg = CmdSubmit(args).run()
            print(msg)
            return

        if args.command == 'submit-file':
            success, msg = CmdSubmitFile(args).run()
            print(msg)
            return

        if args.command == 'view':
            success, msg = CmdView(args).run()
            print(msg)
            return

        # ---- from here on, we require a user profile + Cognito token ----

        if not profile_exists(args.profile):
            print(f"Profile '{args.profile}' not found. Please run config command with your access keys")
            sys.exit(1)

        self.user_profile = get_profile(args.profile)

        access_token = getattr(self.user_profile, "access_token", None)
        if not access_token:
            print("Not authenticated. Run: morphic-util config <username> <password>")
            sys.exit(1)

        backend = os.getenv("STORAGE_BACKEND", "aws").lower()

        if backend == "aws":
            self.aws = Aws(self.user_profile)

            if not self.aws.is_valid_credentials():
                print('Invalid credentials')
                sys.exit(1)

            bucket = get_bucket()
            if bucket:
                self.aws.bucket_name = bucket
            else:
                try:
                    static_bucket_name()
                except Exception:
                    print('Unable to get bucket')
                    sys.exit(1)

            self.storage = build_storage(self.user_profile)

        else:
            # globus backend: no S3 bucket, just storage from factory
            self.storage = build_storage(self.user_profile)

        self.execute(args)

    def check_version(self):
        today = date.today()
        last_checked = get_attr('version_checked')

        if not last_checked or last_checked < today:
            resp = requests.get(f'https://pypi.org/pypi/{NAME}/json')
            latest_version = resp.json()['info']['version']
            if VERSION < latest_version:
                print(f'INFO: A new version of {NAME} is available. Run `pip install {NAME} --upgrade` to upgrade.')
            set_attr('version_checked', today)

    def execute(self, args):
        if args.command == 'create':
            success, msg = CmdCreate(self.storage, args).run()
            self.exit(success, msg)

        elif args.command == 'select':
            success, msg = CmdSelect(self.storage, args).run()
            self.exit(success, msg)

        elif args.command == 'list':
            success, msg = CmdList(self.storage, args).run()
            self.exit(success, msg)

        elif args.command == 'upload':
            success, msg = CmdUpload(self.storage, args).run()
            self.exit(success, msg)

        elif args.command == 'download':
            success, msg = CmdDownload(self.storage, args).run()
            self.exit(success, msg)

        elif args.command == 'delete':
            success, msg = CmdDelete(self.storage, args).run()
            self.exit(success, msg)

        elif args.command == 'sync':
            success, msg = CmdSync(self.storage, args).run()
            self.exit(success, msg)

        else:
            print(f"Unknown command: {args.command}")
            sys.exit(1)

    def exit(self, success, message):
        if message:
            print(message)
        sys.exit(0 if success else 1)
