from ait.commons.util.common import format_err

# import the helpers/constants from your GlobusStorage module
from ait.commons.util.storage.globus_backend import (
    _load_globus_config,
    _save_globus_config,
    _get_transfer_client,
    K_NATIVE_CLIENT_ID,
    K_SRC_UUID,
    K_EBI_UUID,
    K_DEST_ROOT,
    K_API_URL,
    K_API_KEY,
)


class CmdConfigGlobus:
    """
    Configure Globus for morphic-util uploads.

    - Stores config in ~/.morphic-util/config.json
    - Runs Native App auth once to get a refresh token
    """

    def __init__(self, args):
        self.args = args

    def run(self):
        try:
            cfg = _load_globus_config()

            # 1) Update config from CLI args (if provided)
            if getattr(self.args, "native_client_id", None):
                cfg[K_NATIVE_CLIENT_ID] = self.args.native_client_id
            if getattr(self.args, "src_collection_uuid", None):
                cfg[K_SRC_UUID] = self.args.src_collection_uuid
            if getattr(self.args, "ebi_collection_uuid", None):
                cfg[K_EBI_UUID] = self.args.ebi_collection_uuid
            if getattr(self.args, "dest_root", None):
                cfg[K_DEST_ROOT] = self.args.dest_root
            if getattr(self.args, "api_url", None):
                cfg[K_API_URL] = self.args.api_url
            if getattr(self.args, "api_key", None):
                cfg[K_API_KEY] = self.args.api_key

            cfg["storage_backend"] = "globus"
            _save_globus_config(cfg)

            # 2) Trigger Globus Native App auth if refresh_token is missing
            tc = _get_transfer_client(cfg)  # will prompt & then store refresh_token

            # 3) Save updated cfg (with refresh_token)
            _save_globus_config(cfg)

            print("✓ Globus configured for morphic-util CLI.")
            print(f"  SRC collection: {cfg.get(K_SRC_UUID)}")
            print(f"  DEST collection: {cfg.get(K_EBI_UUID)}")
            print(f"  DEST root:       {cfg.get(K_DEST_ROOT)}")
            return True, None

        except Exception as e:
            return False, format_err(e, "config-globus")
