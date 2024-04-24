import requests
import json
from urllib.parse import urlparse

from ait.commons.util.user_profile import get_profile


def get_id_from_url(url):
    parsed_url = urlparse(url)
    path_parts = parsed_url.path.split('/')
    return path_parts[2]


class CmdSubmit:
    base_url = 'http://localhost:8080'

    def __init__(self, args):
        self.args = args
        self.access_token = get_profile('morphic-util').access_token
        self.type = self.args.type
        self.data = None

    def run(self):
        submission_envelope_create_url = f"{self.base_url}/submissionEnvelopes/updateSubmissions"

        if self.type == 'study':
            study_create_url = self.post(submission_envelope_create_url, 'studies')

            submission_envelope_id = get_id_from_url(study_create_url)

            study_create_response = self.post(study_create_url, 'submissionEnvelopes')
            link_study_to_submission_envelope_response = self.put(study_create_response + '/' + submission_envelope_id,
                                                                  'self')

            study_id = get_id_from_url(link_study_to_submission_envelope_response)

            print("Study created successfully: " + study_id)

            return True, study_id
        elif self.type == 'dataset':
            dataset_create_url = self.post(submission_envelope_create_url, 'datasets')

            submission_envelope_id = get_id_from_url(dataset_create_url)

            dataset_create_response = self.post(dataset_create_url, 'submissionEnvelopes')
            link_dataset_to_submission_envelope_response = self.put(
                dataset_create_response + '/' + submission_envelope_id,
                'self')

            dataset_id = get_id_from_url(link_dataset_to_submission_envelope_response)

            # Check if dataset ID is available
            if dataset_id is not None:
                # Check if user has passed study to link, else prompt
                if self.args.study is not None:
                    study_id = self.args.study

                    self.link_dataset_study(dataset_id, study_id)
                # Prompt user to link dataset to study
                else:
                    link_to_study = input("Do you want to link this dataset to a study? (yes/no): ").lower()

                    if link_to_study == 'yes':
                        study_id = input("Input study id: ").lower()
                        self.link_dataset_study(dataset_id, study_id)
            else:
                print("Dataset created successfully.")
            return True, dataset_id

    def link_dataset_study(self, dataset_id, study_id):
        print("Linking dataset " + dataset_id + " to study " + study_id)
        # Perform the linking operation here
        self.put(f"{self.base_url}/studies/{study_id}/datasets/{dataset_id}", None)
        print("Dataset linked successfully to study: " + study_id)

    def post(self, url, data_type_in_hal_link):
        # Read content of the file
        if self.args.file:
            with open(self.args.file, 'r') as file:
                self.data = json.load(file)
        else:
            self.data = {}

        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.access_token}'
        }
        response = requests.post(url, headers=headers, json=self.data)
        response_data = response.json()
        url = response_data['_links'][data_type_in_hal_link]['href']

        return url

    def put(self, url, data_type_in_hal_link):
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.access_token}'
        }
        response = requests.put(url, headers=headers)
        response_data = response.json()

        if data_type_in_hal_link is not None:
            url = response_data['_links'][data_type_in_hal_link]['href']
            return url
