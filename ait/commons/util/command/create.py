from __future__ import annotations

import os
import requests

from ait.commons.util.common import format_err
from ait.commons.util.local_state import set_selected_area
from ait.commons.util.storage.base import Storage


class CmdCreate:
    """
    Create an upload area / project folder.

    Backend behaviour:

      - STORAGE_BACKEND=aws     -> call existing AWS logic (TODO: wire your old code)
      - STORAGE_BACKEND=globus -> call Morphic Storage API /submissions/{name}/init
    """

    def __init__(self, storage: Storage, args):
        self.storage = storage
        self.args = args
        self.backend = os.getenv("STORAGE_BACKEND", "aws").lower()

    def run(self):
        name = self.args.NAME

        try:
            if self.backend == "globus":
                return self._run_globus(name)
            else:
                return self._run_aws(name)
        except Exception as e:
            return False, format_err(e, "create")

    # ----------------- backends -----------------

    def _run_globus(self, name: str):
        """
        Globus/on-prem create:

        - Call Storage API: POST /submissions/{name}/init
        - Mark area as selected locally.
        """
        base_url = os.getenv("MORPHIC_API_URL", "https://api.ingest.dev.archive.morphic.bio")
        api_key = os.getenv("MORPHIC_API_KEY")

        url = base_url.rstrip("/") + f"/submissions/{name}/init"
        headers = {}
        if api_key:
            headers["X-Api-Key"] = api_key

        resp = requests.post(url, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        set_selected_area(name)

        msg = (
            f"Created Globus upload area '{name}'\n"
            f"  collection: {data.get('collection_id')}\n"
            f"  path:       {data.get('upload_path')}"
        )
        return True, msg

    def _run_aws(self, name: str):
        """
        Call your existing AWS-based create logic.

        TODO: Move your old 'create.run()' implementation here.

        For now, we just raise so it's obvious if this path is hit without being implemented.
        """
        raise RuntimeError(
            "AWS create logic not wired in CmdCreate._run_aws yet. "
            "Paste your old create.run() implementation here."
        )
