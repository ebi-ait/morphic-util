import csv
import requests
import json
import pandas as pd
from urllib.parse import urlparse

from ait.commons.util.user_profile import get_profile


def get_id_from_url(url):
    """
    Extracts and returns the ID from a given URL.

    Parameters:
        url (str): The URL string.

    Returns:
        str: The ID extracted from the URL.
    """
    parsed_url = urlparse(url)
    path_parts = parsed_url.path.split('/')
    return path_parts[2]


def get_id(url):
    """
    Extracts and returns the ID from a URL.

    Parameters:
        url (str): The URL string.

    Returns:
        str: The extracted ID or None if an error occurs.
    """
    try:
        id = url.split('/')[-1]
        return id
    except Exception as e:
        print(f"Error encountered: {e}")
        return None


class CmdSubmit:
    """
    A class to handle submission of studies, datasets, and biomaterials to a server.

    Attributes:
        base_url (str): The base URL for the server.
        submission_envelope_create_url (str): URL for creating submission envelopes.
        submission_envelope_base_url (str): Base URL for submission envelopes.
        args (Namespace): Command-line arguments.
        access_token (str): Access token for authorization.
        type (str): Type of submission (study, dataset, or biomaterial).

    Methods:
        run(): Executes the submission process based on the type.
        create_dataset(): Creates a dataset and returns its ID.
        create_biomaterial(): Creates a biomaterial and returns its ID.
        create_study(): Creates a study and returns its ID.
        link_dataset_study(dataset_id, study_id): Links a dataset to a study.
        link_biomaterial_dataset(biomaterial_id, dataset_id): Links a biomaterial to a dataset.
        get_id(url): Extracts and returns the ID from a URL.
        post(url, data_type_in_hal_link): Sends a POST request to the specified URL.
        transform(): Transforms the input file to a JSON object.
        put(url): Sends a PUT request to the specified URL.
    """
    base_url = 'http://localhost:8080'
    submission_envelope_create_url = f"{base_url}/submissionEnvelopes/updateSubmissions"
    submission_envelope_base_url = f"{base_url}/submissionEnvelopes"

    def __init__(self, args):
        """
        Initializes the CmdSubmit class with command-line arguments.

        Parameters:
            args (Namespace): Command-line arguments.
        """
        self.args = args
        self.access_token = get_profile('morphic-util').access_token
        self.type = self.args.type

    def run(self):
        """
        Executes the submission process based on the type of submission.

        Returns:
            tuple: A tuple containing a boolean indicating success and the ID of the created entity.
        """
        if self.type in ['study', 'dataset', 'biomaterial', 'process']:
            entity_id = self.create_entity(self.type)
            if entity_id is not None:
                if self.type == 'dataset':
                    if self.args.study is not None:
                        study_id = self.args.study
                        self.link_dataset_study(entity_id, study_id)
                    else:
                        link_to_study = input("Do you want to link this dataset to a study? (yes/no): ").lower()
                        if link_to_study == 'yes':
                            study_id = input("Input study id: ").lower()
                            self.link_dataset_study(entity_id, study_id)
                elif self.type == 'biomaterial':
                    if self.args.dataset is not None:
                        dataset_id = self.args.dataset
                        self.link_biomaterial_dataset(entity_id, dataset_id)
                    else:
                        link_to_dataset = input("Do you want to link this biomaterial to a dataset? (yes/no): ").lower()
                        if link_to_dataset == 'yes':
                            dataset_id = input("Input dataset id: ").lower()
                            self.link_biomaterial_dataset(entity_id, dataset_id)

                    # Linking biomaterial to process
                    if self.args.process is not None:
                        process_id = self.args.process
                        self.link_biomaterial_process(entity_id, process_id)

                return True, entity_id
        else:
            print("Unsupported type")
        return False, "Unsupported type"

    def create_entity(self, input_entity_type):
        """
        Creates an entity (study, dataset, biomaterial, or process) and returns its ID.

        Parameters:
            input_entity_type (str): The type of entity to create ('study', 'dataset', 'biomaterial', 'process').

        Returns:
            str: The ID of the created entity.
        """
        if input_entity_type == 'study':
            entity = 'studies'
        elif input_entity_type == 'dataset':
            entity = 'datasets'
        elif input_entity_type == 'biomaterial':
            entity = 'biomaterials'
        elif input_entity_type == 'process':
            entity = 'processes'

        entity_create_url_from_sub_env_hal_links = self.post(self.submission_envelope_create_url,
                                                             entity)
        entity_self_hal_link = self.post(entity_create_url_from_sub_env_hal_links, 'self')
        entity_id = get_id_from_url(entity_self_hal_link)
        print(f"{input_entity_type.capitalize()} created successfully: " + entity_id)
        return entity_id

    def create_dataset(self):
        """
        Creates a dataset and returns its ID.

        Returns:
            str: The ID of the created dataset.
        """
        dataset_create_url_from_sub_env_hal_links = self.post(self.submission_envelope_create_url, 'datasets')
        dataset_self_hal_link = self.post(dataset_create_url_from_sub_env_hal_links, 'self')
        dataset_id = get_id(dataset_self_hal_link)
        print("Dataset created successfully: " + dataset_id)
        return dataset_id

    def create_process(self):
        """
        Creates a process and returns its ID.

        Returns:
            str: The ID of the created process.
        """
        process_create_url_from_sub_env_hal_links = self.post(self.submission_envelope_create_url, 'processes')
        process_self_hal_link = self.post(process_create_url_from_sub_env_hal_links, 'self')
        process_id = get_id(process_self_hal_link)
        print("Process created successfully: " + process_id)
        return process_id

    def create_biomaterial(self):
        """
        Creates a biomaterial and returns its ID.

        Returns:
            str: The ID of the created biomaterial.
        """
        biomaterial_create_url_from_sub_env_hal_links = self.post(self.submission_envelope_create_url, 'biomaterials')
        biomaterial_self_hal_link = self.post(biomaterial_create_url_from_sub_env_hal_links, 'self')
        biomaterial_id = get_id(biomaterial_self_hal_link)
        print("Biomaterial created successfully: " + biomaterial_id)
        return biomaterial_id

    def create_study(self):
        """
        Creates a study and returns its ID.

        Returns:
            str: The ID of the created study.
        """
        study_create_url_from_sub_env_hal_links = self.post(self.submission_envelope_create_url, 'studies')
        study_self_hal_link = self.post(study_create_url_from_sub_env_hal_links, 'self')
        study_id = get_id(study_self_hal_link)
        print("Study created successfully: " + study_id)
        return study_id

    def link_dataset_study(self, dataset_id, study_id):
        """
        Links a dataset to a study.

        Parameters:
            dataset_id (str): The ID of the dataset.
            study_id (str): The ID of the study.
        """
        print("Linking dataset " + dataset_id + " to study " + study_id)
        self.put(f"{self.base_url}/studies/{study_id}/datasets/{dataset_id}")
        print("Dataset linked successfully to study: " + study_id)

    def link_biomaterial_dataset(self, biomaterial_id, dataset_id):
        """
        Links a biomaterial to a dataset.

        Parameters:
            biomaterial_id (str): The ID of the biomaterial.
            dataset_id (str): The ID of the dataset.
        """
        print("Linking biomaterial " + biomaterial_id + " to dataset " + dataset_id)
        self.put(f"{self.base_url}/datasets/{dataset_id}/biomaterials/{biomaterial_id}")
        print("Biomaterial linked successfully to dataset: " + dataset_id)

    def link_biomaterial_process(self, biomaterial_id, process_id):
        print("Linking biomaterial " + biomaterial_id + " to process " + process_id)
        self.post_to_link(f"{self.base_url}/biomaterials/{biomaterial_id}/inputToProcesses",
                          process_id, 'biomaterials', 'processes')

    def post(self, url, data_type_in_hal_link):
        """
        Sends a POST request to the specified URL.

        Parameters:
            url (str): The URL to send the request to.
            data_type_in_hal_link (str): The data type in the HAL link.

        Returns:
            str: The URL from the response.
        """
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

    def post_to_link(self, url, input_id, link_this, link_to):
        """
        Sends a POST request to the specified URL.

        Parameters:

        Returns:
        """
        headers = {
            'Content-Type': 'text/uri-list',
            'Authorization': f'Bearer {self.access_token}'
        }

        response = requests.post(url, headers=headers,
                                 data=f"{self.base_url}/{link_to}/{input_id}/{link_this}")

        return response.json()

    def transform(self):
        """
        Transforms the input file to a JSON object.

        Returns:
            dict: The JSON object.
        """
        if self.args.file.endswith('.tsv'):
            json_data = []
            with open(self.args.file, 'r', newline='') as file:
                reader = csv.DictReader(file, delimiter='\t')
                for row in reader:
                    json_data.append(row)
            json_data_formatted = {'content': json_data}
            data = json_data_formatted
        elif self.args.file.endswith('.csv'):
            df = pd.read_csv(self.args.file)
            data = {'content': df.to_dict(orient='records')}
        else:
            with open(self.args.file, 'r') as file:
                data = json.load(file)
        return data

    def put(self, url):
        """
        Sends a PUT request to the specified URL.

        Parameters:
            url (str): The URL to send the request to.

        Returns:
            dict: The response data.
        """
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.access_token}'
        }
        response = requests.put(url, headers=headers)
        response_data = response.json()
        return response_data
