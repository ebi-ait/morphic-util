from typing import Protocol, List, Optional, Callable

ProgressCb = Optional[Callable[[int], None]]


class Storage(Protocol):
    """
    Abstract storage backend.

    Implementations:
      - AwsStorage      (S3 buckets)
      - GlobusStorage   (on-prem via Globus + Morphic Storage API)
    """

    # Upload areas / datasets
    def area_exists(self, area: str) -> bool:
        """Return True if the upload area / dataset exists."""
        ...

    # File-level operations
    def data_file_exists(self, area: str, dest_name: str) -> bool:
        """Check if dest_name exists inside the given area."""
        ...

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
        """Upload (or transfer) a file into the area."""
        ...

    def list(self, area: str, prefix: str = "") -> List[str]:
        """List paths under the area (flat list of names/keys)."""
        ...