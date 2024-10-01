from ait.commons.util.aws_client import Aws
from ait.commons.util.provider_api_util import APIProvider
from ait.commons.util.user_profile import get_profile


class CmdView:
    base_url = 'https://api.ingest.dev.archive.morphic.bio'

    def __init__(self, args):
        self.args = args
        self.access_token = get_profile('morphic-util').access_token
        self.user_profile = get_profile('morphic-util')
        self.provider_api = APIProvider(self.base_url)

        if hasattr(self.args, 'dataset') and self.args.dataset is not None:
            self.dataset = self.args.dataset
        else:
            print("Dataset is mandatory for view")

    def run(self):
        fetched_dataset = self.provider_api.get(f"{self.base_url}/datasets/{self.dataset}",
                                                self.access_token)
        print(f"Dataset fetched successfully: {self.dataset}")
        print("Getting Biomaterials")
        biomaterials = fetched_dataset.get('biomaterials', [])

        for biomaterial in biomaterials:
            print(biomaterial)

            fetched_biomaterial = self.provider_api.get(f"{self.base_url}/biomaterials/{biomaterial}",
                                                        self.access_token)
            print(fetched_biomaterial)

        print("Getting Processes")
        processes = fetched_dataset.get('processes', [])

        for process in processes:
            print(process)

            fetched_process = self.provider_api.get(f"{self.base_url}/processes/{process}",
                                                    self.access_token)
            print(fetched_process)

        print("Getting Data Files")
        files = fetched_dataset.get('files', [])

        for file in files:
            print(files)

            fetched_file = self.provider_api.get(f"{self.base_url}/files/{file}",
                                                 self.access_token)
            print(fetched_file)

        return True, "FETCHED SUCCESSFULLY"
