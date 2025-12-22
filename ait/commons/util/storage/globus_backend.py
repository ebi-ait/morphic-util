from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import List, Optional, Tuple

import globus_sdk as g
from globus_sdk.scopes import TransferScopes
from globus_sdk import AccessTokenAuthorizer
from os.path import basename

import logging
log = logging.getLogger("morphic-util")

from ait.commons.util.user_profile import get_profile
from ait.commons.util.settings.morphic_util import (
    BASE_URL
)

class Storage:
    pass
ProgressCb = callable

# --------------------------
# Configuration & Constants
# --------------------------

CONFIG_DIR = Path.home() / ".morphic-util"
CONFIG_FILE = CONFIG_DIR / "config.json"

K_NATIVE_CLIENT_ID = "native_client_id"
K_REFRESH_TOKEN = "refresh_token"
K_EBI_UUID = "ebi_collection_uuid"
K_DEST_ROOT = "dest_root"
K_SRC_UUID = "src_collection_uuid"
K_API_URL = "api_url"
K_API_KEY = "api_key"
K_STORAGE_BACKEND = "storage_backend"

_WIN_DRIVE_RE = re.compile(r"^([a-zA-Z]):[\\/](.*)$")

SPINNER = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"

# --------------------------
# Helper Functions
# --------------------------

def _to_globus_source_path(p: str) -> str:
    """
    Convert local paths into Globus endpoint paths.
    Windows:
      C:\\Users\\x\\file.txt -> /C/Users/x/file.txt
    Others:
      passthrough with forward slashes
    """
    p = (p or "").strip().strip('"')
    m = _WIN_DRIVE_RE.match(p)
    if m:
        drive = m.group(1).upper()
        rest = m.group(2).replace("\\", "/")
        return f"/{drive}/{rest}"
    return p.replace("\\", "/")

def _human_bytes(n: Optional[int]) -> str:
    """Converts bytes to human-readable format (e.g., 1.2 KB)."""
    if not n:
        return "0 B"
    n = int(n)
    suffixes = ["B", "KB", "MB", "GB", "TB", "PB"]
    i = 0
    x = float(n)
    while x >= 1024 and i < len(suffixes) - 1:
        x /= 1024.0
        i += 1
    return f"{x:.1f} {suffixes[i]}"

def _render_progress_line(
    filename: str,
    status: str,
    nice: Optional[str],
    bytes_done: Optional[int],
    bytes_total: Optional[int],
    files_done: Optional[int],
    files_total: Optional[int],
    rate_bps: Optional[float],
    eta_s: Optional[float],
    spin_idx: int,
) -> str:
    """Renders a single, clean progress line, including the filename and excluding 1/1 file count."""
    parts: list[str] = []

    max_len = 30
    if len(filename) > max_len:
        filename_display = "..." + filename[-(max_len - 3):]
    else:
        filename_display = filename

    parts.append(f"{filename_display:<{max_len}}")

    if status not in ("SUCCEEDED", "FAILED", "CANCELED"):
        parts.append(SPINNER[spin_idx % len(SPINNER)])
    else:
        parts.append("✓" if status == "SUCCEEDED" else "×")

    parts.append(f"| {status}")
    if nice:
        parts.append(f"({nice})")

    bytes_done = bytes_done if bytes_done is not None else 0
    bytes_total = bytes_total if bytes_total is not None else 0
    files_done = files_done if files_done is not None else 0
    files_total = files_total if files_total is not None else 0

    if files_total > 1:
        parts.append(f"files {files_done}/{files_total}")

    if bytes_total > 0:
        pct = (bytes_done / bytes_total) * 100.0
        parts.append(f"{_human_bytes(bytes_done)}/{_human_bytes(bytes_total)} {pct:5.1f}%")
    elif bytes_done > 0:
        parts.append(_human_bytes(bytes_done))

    if rate_bps and rate_bps > 0:
        parts.append(f"[{_human_bytes(int(rate_bps))}/s]")

    if eta_s and eta_s > 1:
        if eta_s > 3600:
            h = int(eta_s // 3600)
            m = int((eta_s % 3600) // 60)
            parts.append(f"ETA {h}h{m:02d}m")
        elif eta_s > 60:
            m = int(eta_s // 60)
            s = int(eta_s % 60)
            parts.append(f"ETA {m}m{s:02d}s")
        else:
            parts.append(f"ETA {int(eta_s)}s")

    return " ".join(parts)

def _autoactivate_patch_noop(
    self: g.TransferClient, endpoint_id: str, if_expires_in: int = 3600
) -> None:
    """Monkey-patch for missing endpoint_autoactivate method (Globus SDK 4.x fix)."""
    try:
        self.get_endpoint_list(num_results=1)
    except Exception:
        return
    return
g.TransferClient.endpoint_autoactivate = _autoactivate_patch_noop


def _load_globus_config() -> dict:
    """Read config.json and overlay environment variables."""
    cfg: dict = {}

    defaults = {
        K_STORAGE_BACKEND: "globus",
        K_NATIVE_CLIENT_ID: os.getenv("MORPHIC_NATIVE_CLIENT_ID", "ada49ae3-31b3-4d2c-9f25-893876ef3952"),
        K_EBI_UUID: os.getenv("MORPHIC_EBI_COLLECTION_UUID", "56c5c4f0-601a-4555-9aca-70f8cacaac0f"),
        K_DEST_ROOT: os.getenv("MORPHIC_DEST_ROOT", "/"),
        K_SRC_UUID: os.getenv("MORPHIC_SRC_COLLECTION_UUID", ""),
        K_API_URL: os.getenv("MORPHIC_API_URL", BASE_URL),
    }

    env_map = {
        K_STORAGE_BACKEND: "STORAGE_BACKEND",
        K_NATIVE_CLIENT_ID: "MORPHIC_NATIVE_CLIENT_ID",
        K_REFRESH_TOKEN: "MORPHIC_REFRESH_TOKEN",
        K_EBI_UUID: "MORPHIC_EBI_COLLECTION_UUID",
        K_DEST_ROOT: "MORPHIC_DEST_ROOT",
        K_SRC_UUID: "MORPHIC_SRC_COLLECTION_UUID",
        K_API_URL: "MORPHIC_API_URL",
        K_API_KEY: "MORPHIC_API_KEY",
    }

    if CONFIG_FILE.exists():
        try:
            with open(CONFIG_FILE) as f:
                cfg.update(json.load(f) or {})
        except Exception:
            pass

    for key, envvar in env_map.items():
        val = os.getenv(envvar)
        if val:
            cfg[key] = val

    for k, v in defaults.items():
        cfg.setdefault(k, v)

    return cfg


def _save_globus_config(cfg: dict) -> None:
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    with open(CONFIG_FILE, "w") as f:
        json.dump(cfg, f, indent=2)


def _norm_dir(p: str) -> str:
    """Normalizes a path to start with / and not end with / (unless it's just /)"""
    p = (p or "/").strip()
    if not p.startswith("/"):
        p = "/" + p
    return p.rstrip("/") or "/"


def _join_path(base: str, name: str) -> str:
    """Joins two path parts, handling normalization for Globus paths."""
    base = _norm_dir(base)
    name = (name or "").lstrip("/")
    return f"{base}/{name}" if name else base


def _qs(**params) -> str:
    cleaned = {k: v for k, v in params.items() if v is not None}
    return "?" + urllib.parse.urlencode(cleaned) if cleaned else ""


def _api_call(cfg: dict, method: str, path: str, data: Optional[dict] = None):
    """Call the Morphic Storage / Provider API."""
    base = cfg.get(K_API_URL)
    if not base:
        raise RuntimeError("Missing api_url in Globus config (MORPHIC_API_URL).")
    url = base.rstrip("/") + path

    req = urllib.request.Request(url, method=method.upper())
    if data is not None:
        req.add_header("Content-Type", "application/json")
        body = json.dumps(data).encode("utf-8")
    else:
        body = None

    # 1) Always use ingest bearer token (Globus access token preferred)
    bearer = _get_ingest_bearer_token(cfg)
    req.add_header("Authorization", f"Bearer {bearer}")

    # Existing API key support (optional / fallback)
    apikey = cfg.get(K_API_KEY) or os.getenv("MORPHIC_API_KEY")
    if apikey:
        req.add_header("X-Api-Key", apikey)

    try:
        with urllib.request.urlopen(req, body) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        msg = e.read().decode("utf-8")
        raise RuntimeError(f"Storage API {method} {url} failed: {e.code} {msg}") from e


def _get_transfer_client(cfg: dict) -> g.TransferClient:
    native_client_id = cfg.get(K_NATIVE_CLIENT_ID)
    if not native_client_id:
        raise RuntimeError("Missing Globus native client ID.")

    ac = g.NativeAppAuthClient(native_client_id)
    rt = cfg.get(K_REFRESH_TOKEN)

    if rt:
        authorizer = g.RefreshTokenAuthorizer(rt, ac)
        return g.TransferClient(authorizer=authorizer)

    ac.oauth2_start_flow(
        requested_scopes=[
            str(TransferScopes.all),
            "openid",
            "profile",
            "email",
        ],
        refresh_tokens=True,
        prefill_named_grant="morphic-util CLI",
    )
    print("Please authenticate with Globus:\n ", ac.oauth2_get_authorize_url())
    auth_code = input("Auth code: ").strip()

    tokens = ac.oauth2_exchange_code_for_tokens(auth_code)
    rs_map = tokens.by_resource_server

    transfer_rs = rs_map["transfer.api.globus.org"]
    auth_rs = rs_map.get("auth.globus.org")

    cfg[K_REFRESH_TOKEN] = transfer_rs["refresh_token"]

    if auth_rs is not None and auth_rs.get("refresh_token"):
        cfg["auth_refresh_token"] = auth_rs["refresh_token"]
        print("[globus] stored auth_refresh_token in config.json", file=sys.stderr)
    else:
        print("[globus] WARNING: no auth_refresh_token returned; check requested scopes", file=sys.stderr)

    try:
        access_token = None
        if auth_rs is not None:
            access_token = auth_rs.get("access_token")

        if not access_token:
            access_token = transfer_rs.get("access_token")

        if access_token:
            auth_authorizer = AccessTokenAuthorizer(access_token)
            authc = g.AuthClient(authorizer=auth_authorizer)
            # We call userinfo mainly as a sanity check; we don't need to persist the subject anymore.
            _ = authc.userinfo()
    except Exception as e:
        print(f"[globus] ERROR calling /userinfo: {e}", file=sys.stderr)

    _save_globus_config(cfg)

    authorizer = g.RefreshTokenAuthorizer(
        cfg[K_REFRESH_TOKEN],
        ac,
        access_token=transfer_rs.get("access_token"),
        expires_at=transfer_rs.get("expires_at_seconds"),
    )
    tc = g.TransferClient(authorizer=authorizer)

    return tc

def debug_print_globus_access_token() -> None:
    """
    Dev helper: print a fresh Globus access token used for ingest API calls.
    """
    cfg = _load_globus_config()
    token = _get_ingest_bearer_token(cfg)

    print("=== Globus access token (for ingest API) ===")
    print(token)
    print("=== END TOKEN ===")

def _get_ingest_bearer_token(cfg: dict) -> str:
    """
    Use the stored Globus AUTH refresh token to get a fresh access token
    for auth.globus.org (used by the ingest / provider API).
    """
    native_client_id = cfg.get(K_NATIVE_CLIENT_ID)
    if not native_client_id:
        raise RuntimeError("Missing Globus native client ID in config.")

    auth_rt = cfg.get("auth_refresh_token")
    if not auth_rt:
        raise RuntimeError(
            "No auth_refresh_token found in config.json. "
            "Run `morphic-util globus-login` to (re)authorise with Globus."
        )

    ac = g.NativeAppAuthClient(native_client_id)

    tokens = ac.oauth2_refresh_token(auth_rt)
    rs_map = tokens.by_resource_server

    auth_rs = rs_map.get("auth.globus.org", {})
    access_token = auth_rs.get("access_token")

    if not access_token:
        raise RuntimeError(
            "Did not get an auth.globus.org access_token from refresh response. "
            "Check requested scopes (openid, profile, email)."
        )

    return access_token


def _try_make_area_dir(
    tc: g.TransferClient,
    dst_endpoint: str,
    base: str,
    area: str,
) -> Tuple[str, bool]:
    """
    Try to create a dedicated subdir for the area: <base>/<area>
    """
    base = _norm_dir(base)
    preferred = _join_path(base, area)

    try:
        tc.operation_mkdir(dst_endpoint, path=preferred)
        return preferred, True
    except Exception:
        try:
            tc.operation_ls(dst_endpoint, path=preferred)
            return preferred, True
        except Exception:
            return base, False

# --------------------------
# Main Storage Class (GlobusStorage)
# --------------------------

class GlobusStorage(Storage):
    """
    On-prem storage via Globus + Morphic Storage API.
    """

    def __init__(self):
        self.cfg = _load_globus_config()
        self._tc: g.TransferClient | None = None
        self._activated: bool = False

        try:
            if "auth_refresh_token" not in self.cfg:
                profile = get_profile("morphic-util")
                token = getattr(profile, "access_token", None)
                if token:
                    self.cfg["access_token"] = token
        except Exception:
            pass

    def _print_area_context(self, op: str, area: str) -> None:
            """
            Small helper to print which area an operation is acting on.
            """
            print(f"[GlobusStorage] AREA {area} — {op}")

    @property
    def tc(self) -> g.TransferClient:
        if self._tc is None:
            self._tc = _get_transfer_client(self.cfg)

        if not self._activated:
            try:
                self._tc.endpoint_autoactivate(self.src_endpoint, if_expires_in=3600)
                self._tc.endpoint_autoactivate(self.dst_endpoint, if_expires_in=3600)
                self._activated = True
            except Exception:
                pass

        return self._tc

    @property
    def src_endpoint(self) -> str:
        ep = self.cfg.get(K_SRC_UUID)
        if not ep:
            raise RuntimeError("Missing source collection UUID.")
        return ep

    @property
    def dst_endpoint(self) -> str:
        ep = self.cfg.get(K_EBI_UUID)
        if not ep:
            raise RuntimeError("Missing EBI collection UUID.")
        return ep

    @property
    def dest_root(self) -> str:
        """
        Effective root on the EBI collection where submissions live (for API calls).
        """
        base = self.cfg.get(K_DEST_ROOT, "/")
        subdir = os.getenv("MORPHIC_UPLOAD_SUBDIR", "")
        if subdir:
            base = _join_path(base, subdir)
        return _norm_dir(base)

    def area_exists(self, area: str) -> bool:
        """
        Check if the dataset's Globus upload area exists by calling the provider API.
        """
        try:
            res = _api_call(self.cfg, "GET", f"/datasets/{area}/globus/area-exists")
            exists = bool(res.get("exists"))
            return exists
        except Exception as e:
            print(f"[GlobusStorage] area_exists error for {area}: {e}", file=sys.stderr)
            return False

    def data_file_exists(self, area: str, dest_name: str) -> bool:
        """
        Check if a file exists in the dataset's Globus upload area via API.
        """
        base = self.cfg.get(K_API_URL)
        if not base:
            raise RuntimeError("Missing api_url in Globus config (MORPHIC_API_URL).")

        url = (
            base.rstrip("/")
            + f"/datasets/{area}/globus/files/exists"
            + _qs(path=dest_name.lstrip("/"))
        )

        req = urllib.request.Request(url, method="GET")

        # Use Globus bearer token (same as _api_call)
        bearer = _get_ingest_bearer_token(self.cfg)
        req.add_header("Authorization", f"Bearer {bearer}")

        # Existing API key fallback
        apikey = self.cfg.get(K_API_KEY) or os.getenv("MORPHIC_API_KEY")
        if apikey:
            req.add_header("X-Api-Key", apikey)

        try:
            with urllib.request.urlopen(req) as resp:
                ok = (resp.status // 100) == 2
                return ok
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return False
            msg = e.read().decode("utf-8", errors="ignore")
            raise RuntimeError(
                f"data_file_exists failed: {e.code} {msg}"
            ) from e

    def upload_file(
            self,
            area: str,
            local_path: str,
            dest_name: str,
            file_index: int,
            total_files: int,
            *,
            content_type: str | None,
            md5_hex: str | None,
            overwrite: bool = False,
            progress_cb: ProgressCb = None,
        ) -> None:
            """
            Upload a single file via Globus, showing its index relative to the total batch.
            """

            self._print_area_context(
               f"uploading [{file_index}/{total_files}] -> {dest_name}",
               area,
            )
            src_ep = self.src_endpoint
            dst_ep = self.dst_endpoint

            tc = self.tc

            api_area_root = self.dest_root

            area_dir, subdir_supported = _try_make_area_dir(tc, dst_ep, api_area_root, area)

            dest_name = dest_name.lstrip("/")
            file_basename = os.path.basename(local_path)

            if subdir_supported:
                dest_path = _join_path(area_dir, dest_name)
            else:
                dest_path = _join_path(api_area_root, f"{area}__{basename(dest_name)}")

            tdata = g.TransferData(
                source_endpoint=src_ep,
                destination_endpoint=dst_ep,
                label=f"morphic {area}",
                verify_checksum=True,
                notify_on_failed=True,
            )
            src_path = _to_globus_source_path(local_path)
            log.debug(f"Globus source path resolved to: {src_path}")
            tdata.add_item(src_path, dest_path)

            res = tc.submit_transfer(tdata)
            task_id = res["task_id"]

            start_t = time.time()
            last_bytes: Optional[int] = 0
            last_t = start_t
            spin = 0

            try:
                guessed_total: Optional[int] = os.path.getsize(local_path)
            except Exception:
                guessed_total = None

            try:
                while True:
                    t = tc.get_task(task_id)
                    status = t.get("status", "?")
                    nice = t.get("nice_status_short_description")

                    bytes_done = t.get("bytes_transferred")
                    bytes_total = t.get("bytes_expected") or guessed_total
                    files_total_task = t.get("files")
                    files_done_task = t.get("files_transferred") if t.get("files_transferred") is not None else 0

                    now = time.time()
                    dt = max(1e-6, now - last_t)

                    safe_last_bytes = last_bytes if last_bytes is not None else 0

                    if bytes_done is not None:
                        dbytes = bytes_done - safe_last_bytes
                        rate = dbytes / dt if dbytes >= 0 else None
                    else:
                        rate = None

                    eta = None
                    if rate and rate > 0 and bytes_total and bytes_done is not None and bytes_total > bytes_done:
                        eta = (bytes_total - bytes_done) / rate

                    line = _render_progress_line(
                        filename=file_basename,
                        status=status, nice=nice, bytes_done=bytes_done, bytes_total=bytes_total,
                        files_done=files_done_task, files_total=files_total_task, rate_bps=rate, eta_s=eta,
                        spin_idx=spin,
                    )

                    sys.stdout.write("\r" + line.ljust(100))
                    sys.stdout.flush()

                    last_t = now
                    last_bytes = bytes_done if bytes_done is not None else last_bytes
                    spin += 1

                    if status in ("SUCCEEDED", "FAILED", "CANCELED"):
                        sys.stdout.write("\r" + line.ljust(100) + "\n")
                        sys.stdout.flush()

                        if status != "SUCCEEDED":
                            raise RuntimeError(
                                f"Globus transfer failed: status={status}, task_id={task_id}, nice={nice}"
                            )
                        break

                    time.sleep(1.0)

            except KeyboardInterrupt:
                print(
                        f"\nUpload interrupted by user. "
                        f"Transfer task may still be running on Globus (task_id={task_id})."
                )
                raise

    def list(self, area: str, prefix: str = "") -> List[str]:
        """
        List files in a dataset's Globus area via the provider API.
        This expects a JSON list of {name, type, size} dicts,
        as returned by /datasets/{id}/globus/files.
        """
        self.tc

        self._print_area_context(f"listing files (prefix='{prefix}')", area)

        try:
            raw = _api_call(
                self.cfg,
                "GET",
                f"/datasets/{area}/globus/files" + _qs(prefix=prefix or None),
            )

            if not isinstance(raw, list):
                raise RuntimeError(f"Unexpected list response shape (expected JSON array): {raw!r}")

            file_data = raw

            lines: list[str] = []
            lines.append(f"{'Name':<40} {'Size':>10} {'Type':<10}")
            lines.append(f"{'-'*40:<40} {'-'*10:>10} {'-'*10:<10}")

            for entry in file_data:
                if not isinstance(entry, dict):
                    name = str(entry)
                    file_type = "file"
                    size = None
                else:
                    name = entry.get("name", "N/A")
                    file_type = entry.get("type")
                    size = entry.get("size")

                size_str = _human_bytes(size) if size is not None else ""

                if file_type == "dir":
                    lines.append(f"{name:<40} {'':>10} {'dir':<10}")
                else:
                    lines.append(f"{name:<40} {size_str:>10} {'file':<10}")

            return lines

        except Exception as e:
            raise RuntimeError(f"List files failed for dataset {area}: {e}") from e


    def delete(self, area: str, paths: Optional[List[str]] = None, all_contents: bool = False) -> None:
        """
        Deletes specific files/directories or all contents of a dataset area
        by calling the Provider API endpoint POST /datasets/{area}/delete.
        """
        if not paths and not all_contents:
            raise ValueError("Must provide paths or set all_contents=True for deletion.")

        if all_contents:
            self._print_area_context("deleting ALL contents", area)
        else:
            self._print_area_context(f"deleting paths: {paths}", area)

        payload = {
            "paths": paths,
            "all_contents": all_contents,
            # Assume recursive is True for files, matching API default
            "recursive": True,
        }

        api_path = f"/datasets/{area}/delete"
        try:
            res = _api_call(
                self.cfg,
                "POST",
                api_path,
                data=payload
            )
            task_id = res.get("delete_task_id")
            targets = res.get("targets")

            target_summary = str(len(targets)) if isinstance(targets, list) else str(targets)

            print(f"Deletion task submitted successfully.")
            print(f"Targets deleted: {target_summary}")

        except RuntimeError as e:
            raise RuntimeError(f"API Deletion failed for dataset {area}: {e}")