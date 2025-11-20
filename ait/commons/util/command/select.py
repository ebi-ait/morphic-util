from ait.commons.util.common import format_err
from ait.commons.util.local_state import get_selected_area, set_selected_area
from ait.commons.util.storage.base import Storage


class CmdSelect:
    """
    Select or show the active upload area.

    Backend-agnostic:

      - For AWS (AwsStorage), area_exists() checks if the S3 bucket exists.
      - For Globus (GlobusStorage), area_exists() checks via Storage API
        /submissions/{area}/exists?path=/.
    """

    def __init__(self, storage: Storage, args):
        self.storage = storage
        self.args = args

    def run(self):
        try:
            if self.args.AREA:
                key = self.args.AREA

                if self.storage.area_exists(key):
                    set_selected_area(key)
                    return True, f"Selected upload area is {key}"
                else:
                    return False, f"Upload area does not exist - {key}"
            else:
                selected_area = get_selected_area()
                if selected_area:
                    return True, "Currently selected upload area is " + selected_area
                else:
                    return False, "No upload area currently selected"

        except Exception as e:
            return False, format_err(e, "select")
