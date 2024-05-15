import csv

import requests
import json
import pandas as pd
from urllib.parse import urlparse

from ait.commons.util.user_profile import get_profile


def get_id_from_url(url):
    parsed_url = urlparse(url)
    path_parts = parsed_url.path.split('/')
    return path_parts[2]


class CmdSubmit:
    base_url = 'https://api.ingest.dev.archive.morphic.bio/'

    def __init__(self, args):
        self.args = args
        self.access_token = get_profile('morphic-util').access_token
        self.type = self.args.type

    def run(self):
        submission_envelope_create_url = f"{self.base_url}/submissionEnvelopes/updateSubmissions"

        if self.type == 'study':
            study_id = self.create_study(submission_envelope_create_url)

            return True, study_id
        elif self.type == 'dataset':
            dataset_id = self.create_dataset(submission_envelope_create_url)

            print("Dataset created successfully: " + dataset_id)

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
                return True, dataset_id
        else:
            print("Unsupported type")
        return False, "Unsupported type"

    def create_dataset(self, submission_envelope_create_url):
        dataset_create_url = self.post(submission_envelope_create_url, 'datasets')
        submission_envelope_id = get_id_from_url(dataset_create_url)
        dataset_create_response = self.post(dataset_create_url, 'submissionEnvelopes')
        link_dataset_to_submission_envelope_response = self.put(
            dataset_create_response + '/' + submission_envelope_id,
            'self')
        dataset_id = get_id_from_url(link_dataset_to_submission_envelope_response)
        return dataset_id

    def create_study(self, submission_envelope_create_url):
        study_create_url = self.post(submission_envelope_create_url, 'studies')
        submission_envelope_id = get_id_from_url(study_create_url)
        study_create_response = self.post(study_create_url, 'submissionEnvelopes')
        link_study_to_submission_envelope_response = self.put(study_create_response + '/' + submission_envelope_id,
                                                              'self')
        study_id = get_id_from_url(link_study_to_submission_envelope_response)
        print("Study created successfully: " + study_id)
        return study_id

    def link_dataset_study(self, dataset_id, study_id):
        print("Linking dataset " + dataset_id + " to study " + study_id)
        # Perform the linking operation here
        self.put(f"{self.base_url}/studies/{study_id}/datasets/{dataset_id}", None)
        print("Dataset linked successfully to study: " + study_id)

    def post(self, url, data_type_in_hal_link):
        # Read content of the file
        if self.args.file:
            data = self.transform()
        else:
            data = {}

        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.access_token}'
        }
        response = requests.post(url, headers=headers, json=data)
        response_data = response.json()
        url = response_data['_links'][data_type_in_hal_link]['href']

        return url

    def transform(self):
        if self.args.file.endswith('.tsv'):
            # Read TSV file and convert to JSON
            json_data = []

            with open(self.args.file, 'r', newline='') as file:
                reader = csv.DictReader(file, delimiter='\t')
                for row in reader:
                    json_data.append(row)

            # Ensure JSON data is properly formatted
            json_data_formatted = {'content': json_data}

            # Assign formatted JSON data to self.data
            data = json_data_formatted
        elif self.args.file.endswith('.csv'):
            # Read CSV file and convert to JSON
            df = pd.read_csv(self.args.file)
            data = {'content': df.to_dict(orient='records')}
        else:
            # Read JSON file
            with open(self.args.file, 'r') as file:
                data = json.load(file)

        return data

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
