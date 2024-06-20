import csv
import requests
import json
import pandas as pd
import numpy as np
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
        multi_type_submission(cell_lines, submission_envelope_id, access_token): Submits multiple cell lines.
        typed_submission(type, file, access_token): Submits a single entity based on its type.
        create_new_envelope_and_submit_entity(input_entity_type, data, access_token): Creates and submits a new entity.
        use_existing_envelope_and_submit_entity(input_entity_type, data, submission_envelope_id, access_token): Submits an entity using an existing envelope.
        link_dataset_to_study(dataset_id, study_id, access_token): Links a dataset to a study.
        link_biomaterial_to_dataset(biomaterial_id, dataset_id, access_token): Links a biomaterial to a dataset.
        link_biomaterial_to_process(biomaterial_id, process_id, access_token): Links a biomaterial to a process.
        post_to_provider_api(url, data_type_in_hal_link, data, access_token): Sends a POST request to the provider API.
        create_new_submission_envelope(url, access_token): Creates a new submission envelope.
        perform_hal_linkage(url, input_id, link_this, link_to, access_token): Performs HAL linkage.
        transform(file): Transforms the input file to a JSON object.
        put_to_provider_api(url, access_token): Sends a PUT request to the provider API.
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
        self.type = getattr(self.args, 'type', None)
        self.file = getattr(self.args, 'file', None)

    def run(self):
        """
        Executes the submission process based on the type of submission.

        Returns:
            tuple: A tuple containing a boolean indicating success and the ID of the created entity.
        """
        return self.typed_submission(self.type, self.file, self.access_token)

    def submit_cell_line(self, cell_line, submission_envelope_id, access_token):
        """
        Submits a cell line as a biomaterial entity to a specified submission envelope.

        Parameters:
        - cell_line: The cell line object to be submitted.
        - submission_envelope_id: ID of the submission envelope where the entity will be submitted.
        - access_token: Access token for authentication and authorization.

        Returns:
        - cell_line_entity_id: Entity ID of the submitted cell line biomaterial.
        """
        cell_line_entity_id = self.use_existing_envelope_and_submit_entity(
            'biomaterial',
            cell_line.to_dict(),
            submission_envelope_id,
            access_token
        )
        return cell_line_entity_id

    def handle_differentiated_cell_lines(self, cell_line, cell_line_entity_id, differentiated_cell_lines_df,
                                         library_preparations_df, sequencing_file_df, submission_envelope_id,
                                         access_token):
        """
        Handles differentiated cell lines associated with a given cell line.

        Parameters:
        - cell_line: The main cell line object.
        - cell_line_entity_id: Entity ID of the main cell line already submitted.
        - differentiated_cell_lines_df: DataFrame containing information about differentiated cell lines.
        - library_preparations_df: DataFrame containing information about library preparations.
        - submission_envelope_id: ID of the submission envelope where entities will be linked.
        - access_token: Access token for authentication and authorization.
        """
        if len(cell_line.differentiated_cell_lines) > 0:
            print("Cell line has differentiated cell lines, creating process to link them")
            differentiation_process_entity_id = self.use_existing_envelope_and_submit_entity(
                'process', {}, submission_envelope_id, access_token
            )

            differentiated_biomaterial_to_entity_id_map = {}
            differentiated_cell_line_entity_id_column_name = "differentiated_cell_line_entity_id"

            if differentiated_cell_line_entity_id_column_name not in differentiated_cell_lines_df.columns:
                differentiated_cell_lines_df[differentiated_cell_line_entity_id_column_name] = np.nan

            for differentiated_cell_line in cell_line.differentiated_cell_lines:
                print(f"Creating Differentiated Cell Line Biomaterial: "
                      f"{differentiated_cell_line.biomaterial_id} as a child of Cell line: "
                      f"{cell_line_entity_id}")

                differentiated_entity_id = self.create_child_biomaterial(
                    cell_line_entity_id,
                    differentiated_cell_line.to_dict(),
                    access_token
                )

                print(f"Created Differentiated Cell Line Biomaterial: "
                      f"{differentiated_entity_id}")

                print(f"Linking Differentiated Cell Line Biomaterial: "
                      f"{differentiated_entity_id} to envelope: "
                      f"{submission_envelope_id}")

                self.link_entity_to_envelope(
                    'biomaterial',
                    differentiated_entity_id,
                    submission_envelope_id,
                    access_token
                )

                print(f"Linking Cell Line Biomaterial: "
                      f"{cell_line_entity_id} as input to process : "
                      f"{differentiation_process_entity_id}")

                self.perform_hal_linkage(
                    f"{self.base_url}/biomaterials/{cell_line_entity_id}/inputToProcesses",
                    differentiation_process_entity_id, 'processes', access_token
                )

                print(f"Linking Differentiated cell line Biomaterial: "
                      f"{differentiated_entity_id} as derived by process : "
                      f"{differentiation_process_entity_id}")

                self.perform_hal_linkage(
                    f"{self.base_url}/biomaterials/{differentiated_entity_id}/derivedByProcesses",
                    differentiation_process_entity_id, 'processes', access_token
                )

                differentiated_biomaterial_to_entity_id_map[
                    differentiated_cell_line.biomaterial_id] = differentiated_entity_id

                differentiated_cell_lines_df.loc[
                    differentiated_cell_lines_df[
                        'differentiated_cell_line.biomaterial_core.biomaterial_id'] == differentiated_cell_line.biomaterial_id,
                    differentiated_cell_line_entity_id_column_name
                ] = differentiated_entity_id

                self.handle_library_preparations(differentiated_cell_line,
                                                 differentiated_entity_id,
                                                 library_preparations_df,
                                                 sequencing_file_df,
                                                 submission_envelope_id,
                                                 access_token)

    def handle_library_preparations(self, differentiated_cell_line, differentiated_entity_id,
                                    library_preparations_df, sequencing_file_df, submission_envelope_id, access_token):
        """
        Handles library preparations associated with a differentiated cell line.

        Parameters:
        - differentiated_cell_line: The differentiated cell line object.
        - differentiated_entity_id: Entity ID of the differentiated cell line.
        - library_preparations_df: DataFrame containing information about library preparations.
        - submission_envelope_id: ID of the submission envelope where entities will be linked.
        - access_token: Access token for authentication and authorization.
        """
        if len(differentiated_cell_line.library_preparations) > 0:
            print("Differentiated cell line has library preparation biomaterials, creating process to link them")
            library_preparation_process_entity_id = self.use_existing_envelope_and_submit_entity(
                'process', {}, submission_envelope_id, access_token
            )

            library_preparation_biomaterial_to_entity_id_map = {}
            library_preparation_biomaterial_entity_id_column_name = " library_preparation_biomaterial_entity_id"

            if library_preparation_biomaterial_entity_id_column_name not in library_preparations_df.columns:
                library_preparations_df[library_preparation_biomaterial_entity_id_column_name] = np.nan

            for library_preparation in differentiated_cell_line.library_preparations:
                print(f"Creating Library Preparation Biomaterial: "
                      f"{library_preparation.biomaterial_id} as a child of Differentiated Cell line: "
                      f"{differentiated_entity_id}")

                library_preparation_entity_id = self.create_child_biomaterial(
                    differentiated_entity_id,
                    library_preparation.to_dict(),
                    access_token
                )

                print(f"Created Library preparation Biomaterial: "
                      f"{library_preparation_entity_id}")

                print(f"Linking Library preparation Biomaterial: "
                      f"{library_preparation_entity_id} to envelope: "
                      f"{submission_envelope_id}")

                self.link_entity_to_envelope(
                    'biomaterial',
                    library_preparation_entity_id,
                    submission_envelope_id,
                    access_token
                )

                print(f"Linking Differentiation cell line Biomaterial: "
                      f"{differentiated_entity_id} as input to process : "
                      f"{library_preparation_process_entity_id}")

                self.perform_hal_linkage(
                    f"{self.base_url}/biomaterials/{differentiated_entity_id}/inputToProcesses",
                    library_preparation_process_entity_id, 'processes', access_token
                )

                print(f"Linking Library preparation Biomaterial: "
                      f"{library_preparation_entity_id} as derived by process : "
                      f"{library_preparation_process_entity_id}")

                self.perform_hal_linkage(
                    f"{self.base_url}/biomaterials/{library_preparation_entity_id}/derivedByProcesses",
                    library_preparation_process_entity_id, 'processes', access_token
                )

                library_preparation_biomaterial_to_entity_id_map[
                    library_preparation.biomaterial_id] = library_preparation_entity_id

                library_preparations_df.loc[
                    library_preparations_df[
                        'library_preparation.biomaterial_core.biomaterial_id'] == library_preparation.biomaterial_id,
                    library_preparation_biomaterial_entity_id_column_name
                ] = library_preparation_entity_id

                self.handle_sequence_files(library_preparation,
                                           library_preparation_entity_id,
                                           sequencing_file_df,
                                           submission_envelope_id,
                                           access_token)

    def handle_sequence_files(self, library_preparation, library_preparation_entity_id,
                              sequencing_file_df,
                              submission_envelope_id,
                              access_token):
        if len(library_preparation.sequencing_files) > 0:
            print("library preparation has sequencing files, creating process to link them")
            sequencing_process_entity_id = self.use_existing_envelope_and_submit_entity(
                'process', {}, submission_envelope_id, access_token
            )

            sequencing_file_to_entity_id_map = {}
            sequencing_file_entity_id_column_name = "sequencing_file_entity_id"

            if sequencing_file_entity_id_column_name not in sequencing_file_df.columns:
                sequencing_file_df[sequencing_file_entity_id_column_name] = np.nan

            for sequencing_file in library_preparation.sequencing_files:
                print(f"Creating Sequencing file: "
                      f"{sequencing_file.file_name} as a result of sequencing the "
                      f"Library preparation biomaterial: "
                      f"{library_preparation_entity_id}")

                sequencing_file_entity_id = self.use_existing_envelope_and_submit_entity(
                    'file',
                    sequencing_file.to_dict(),
                    submission_envelope_id,
                    access_token
                )

                print(f"Created Sequencing file: "
                      f"{sequencing_file_entity_id}")

                print(f"Linking Library preparation Biomaterial: "
                      f"{library_preparation_entity_id} as input to process : "
                      f"{sequencing_process_entity_id}")

                self.perform_hal_linkage(
                    f"{self.base_url}/biomaterials/{library_preparation_entity_id}/inputToProcesses",
                    sequencing_process_entity_id, 'processes', access_token
                )

                print(f"Linking Sequencing file: "
                      f"{sequencing_file_entity_id} as derived by process : "
                      f"{sequencing_process_entity_id}")

                self.perform_hal_linkage(
                    f"{self.base_url}/files/{sequencing_file_entity_id}/derivedByProcesses",
                    sequencing_process_entity_id, 'processes', access_token
                )

                sequencing_file_df.loc[
                    sequencing_file_df[
                        'sequence_file.file_core.file_name'] == sequencing_file.file_name,
                    sequencing_file_entity_id_column_name
                ] = sequencing_file_entity_id

                sequencing_file_to_entity_id_map[
                    sequencing_file.file_name] = sequencing_file_entity_id

    def multi_type_submission(self, cell_lines, cell_lines_df,
                              differentiated_cell_lines_df,
                              library_preparations_df,
                              sequencing_file_df,
                              submission_envelope_id,
                              access_token):
        """
        Handles the submission of multiple types of biomaterials (cell lines, differentiated cell lines, library preparations)
        to a specified submission envelope.

        Parameters:
        - cell_lines: List of cell line objects to be submitted.
        - cell_lines_df: DataFrame for tracking cell line entity IDs.
        - differentiated_cell_lines_df: DataFrame for tracking differentiated cell line entity IDs.
        - library_preparations_df: DataFrame for tracking library preparation entity IDs.
        - submission_envelope_id: ID of the submission envelope where entities will be linked.
        - access_token: Access token for authentication and authorization.

        Returns:
        - Updated versions of cell_lines_df, differentiated_cell_lines_df, and library_preparations_df after submission.
        """
        cell_line_entity_id_column_name = "cell_line_entity_id"

        if cell_line_entity_id_column_name not in cell_lines_df.columns:
            cell_lines_df[cell_line_entity_id_column_name] = np.nan

        for cell_line in cell_lines:
            print(f"Creating Cell Line Biomaterial: {cell_line.biomaterial_id}")

            cell_line_entity_id = self.submit_cell_line(cell_line, submission_envelope_id, access_token)
            cell_lines_df.loc[
                cell_lines_df['cell_line.biomaterial_core.biomaterial_id'] == cell_line.biomaterial_id,
                cell_line_entity_id_column_name
            ] = cell_line_entity_id

            self.handle_differentiated_cell_lines(cell_line, cell_line_entity_id, differentiated_cell_lines_df,
                                                  library_preparations_df, sequencing_file_df, submission_envelope_id,
                                                  access_token)

        return cell_lines_df, differentiated_cell_lines_df, library_preparations_df, sequencing_file_df

    def typed_submission(self, type, file, access_token):
        """
        Submits a single entity based on its type.

        Parameters:
            type (str): The type of entity to be submitted ('study', 'dataset', 'biomaterial', 'process').
            file (str): The file containing the data to be submitted.
            access_token (str): Access token for authorization.

        Returns:
            tuple: A tuple containing a boolean indicating success and the ID of the created entity.
        """
        if type in ['study', 'dataset', 'biomaterial', 'process']:
            if file is not None:
                data = self.transform(file)
            else:
                data = {}

            entity_id = self.create_new_envelope_and_submit_entity(type, data, access_token)

            if entity_id is not None:
                if type == 'dataset':
                    if self.args.study is not None:
                        study_id = self.args.study
                        self.link_dataset_to_study(entity_id, study_id, access_token)
                    else:
                        link_to_study = input("Do you want to link this dataset to a study? (yes/no): ").lower()
                        if link_to_study == 'yes':
                            study_id = input("Input study id: ").lower()
                            self.link_dataset_to_study(entity_id, study_id, access_token)

                elif type == 'biomaterial':
                    if self.args.dataset is not None:
                        dataset_id = self.args.dataset
                        self.link_biomaterial_to_dataset(entity_id, dataset_id, access_token)
                    else:
                        link_to_dataset = input("Do you want to link this biomaterial to a dataset? (yes/no): ").lower()
                        if link_to_dataset == 'yes':
                            dataset_id = input("Input dataset id: ").lower()
                            self.link_biomaterial_to_dataset(entity_id, dataset_id, access_token)

                    # Linking biomaterial to process
                    if self.args.process is not None:
                        process_id = self.args.process
                        self.link_biomaterial_to_process(entity_id, process_id, access_token)

                return True, entity_id
        else:
            print("Unsupported type")
        return False, "Unsupported type"

    def create_new_envelope_and_submit_entity(self, input_entity_type, data, access_token):
        """
        Creates and submits a new entity (study, dataset, biomaterial, or process) and returns its ID.

        Parameters:
            input_entity_type (str): The type of entity to create ('study', 'dataset', 'biomaterial', 'process').
            data (dict): The data to be submitted.
            access_token (str): Access token for authorization.

        Returns:
            str: The ID of the created entity.
        """
        if input_entity_type == 'study':
            halEntity = 'studies'
        elif input_entity_type == 'dataset':
            halEntity = 'datasets'
        elif input_entity_type == 'biomaterial':
            halEntity = 'biomaterials'
        elif input_entity_type == 'process':
            halEntity = 'processes'

        entity_create_url_from_sub_env_hal_links = self.post_to_provider_api(self.submission_envelope_create_url,
                                                                             halEntity, None, access_token)
        entity_self_hal_link = self.post_to_provider_api(entity_create_url_from_sub_env_hal_links,
                                                         'self', data, access_token)
        entity_id = get_id_from_url(entity_self_hal_link)

        print(f"{input_entity_type.capitalize()} created successfully: " + entity_id)

        return entity_id

    def use_existing_envelope_and_submit_entity(self, input_entity_type, data, submission_envelope_id, access_token):
        """
        Submits an entity using an existing submission envelope and returns its ID.

        Parameters:
            input_entity_type (str): The type of entity to create ('study', 'dataset', 'biomaterial', 'process').
            data (dict): The data to be submitted.
            submission_envelope_id (str): ID of the submission envelope.
            access_token (str): Access token for authorization.

        Returns:
            str: The ID of the created entity.
        """
        if input_entity_type == 'study':
            halEntity = 'studies'
        elif input_entity_type == 'dataset':
            halEntity = 'datasets'
        elif input_entity_type == 'biomaterial':
            halEntity = 'biomaterials'
        elif input_entity_type == 'process':
            halEntity = 'processes'
        elif input_entity_type == 'file':
            halEntity = 'files'

        entity_create_url_from_sub_env_hal_links = (self.submission_envelope_base_url
                                                    + "/" + submission_envelope_id
                                                    + "/" + halEntity)
        entity_self_hal_link = self.post_to_provider_api(entity_create_url_from_sub_env_hal_links,
                                                         'self', data, access_token)
        entity_id = get_id_from_url(entity_self_hal_link)

        print(f"{input_entity_type.capitalize()} created successfully: " + entity_id)

        return entity_id

    def link_dataset_to_study(self, dataset_id, study_id, access_token):
        """
        Links a dataset to a study.

        Parameters:
            dataset_id (str): The ID of the dataset.
            study_id (str): The ID of the study.
            access_token (str): Access token for authorization.
        """
        print("Linking dataset " + dataset_id + " to study " + study_id)

        self.put_to_provider_api(f"{self.base_url}/studies/{study_id}/datasets/{dataset_id}", access_token)

        print("Dataset linked successfully to study: " + study_id)

    def link_biomaterial_to_dataset(self, biomaterial_id, dataset_id, access_token):
        """
        Links a biomaterial to a dataset.

        Parameters:
            biomaterial_id (str): The ID of the biomaterial.
            dataset_id (str): The ID of the dataset.
            access_token (str): Access token for authorization.
        """
        print("Linking biomaterial " + biomaterial_id + " to dataset " + dataset_id)

        self.put_to_provider_api(f"{self.base_url}/datasets/{dataset_id}/biomaterials/{biomaterial_id}", access_token)

        print("Biosmaterial linked successfully to dataset: " + dataset_id)

    def link_biomaterial_to_process(self, biomaterial_id, process_id, access_token):
        """
        Links a biomaterial to a process.

        Parameters:
            biomaterial_id (str): The ID of the biomaterial.
            process_id (str): The ID of the process.
            access_token (str): Access token for authorization.
        """
        print("Linking biomaterial " + biomaterial_id + " to process " + process_id)

        self.perform_hal_linkage(f"{self.base_url}/biomaterials/{biomaterial_id}/inputToProcesses",
                                 process_id, 'processes', access_token)

    def post_to_provider_api(self, url, data_type_in_hal_link, data, access_token):
        """
        Sends a POST request to the specified URL.

        Parameters:
            url (str): The URL to send the request to.
            data_type_in_hal_link (str): The data type in the HAL link.
            data (dict): The data to be sent in the POST request.
            access_token (str): Access token for authorization.

        Returns:
            str: The URL from the response.
        """
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {access_token}'
        }

        response = requests.post(url, headers=headers, json=data)
        response_data = response.json()
        url = response_data['_links'][data_type_in_hal_link]['href']

        return url

    def post_to_provider_api_and_get_entity_id(self, url, data, access_token):
        """
        Sends a POST request to the specified URL.

        Parameters:
            url (str): The URL to send the request to.
            data_type_in_hal_link (str): The data type in the HAL link.
            data (dict): The data to be sent in the POST request.
            access_token (str): Access token for authorization.

        Returns:
            str: The URL from the response.
        """
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {access_token}'
        }

        response = requests.post(url, headers=headers, json=data)
        response_data = response.json()
        url = response_data['_links']['self']['href']

        return get_id_from_url(url)

    def create_new_submission_envelope(self, url, access_token):
        """
        Creates a new submission envelope.

        Parameters:
            url (str): The URL to send the request to.
            access_token (str): Access token for authorization.

        Returns:
            dict: The response data.
        """
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {access_token}'
        }

        response = requests.post(url, headers=headers, json={})
        response_data = response.json()

        return response_data

    def perform_hal_linkage(self, url, input_id, link_to, access_token):
        """
        Performs HAL linkage.

        Parameters:
            url (str): The URL to send the request to.
            input_id (str): The ID of the input entity.
            link_this (str): The entity to link.
            link_to (str): The entity to link to.
            access_token (str): Access token for authorization.

        Returns:
            dict: The response data.
        """
        headers = {
            'Content-Type': 'text/uri-list',
            'Authorization': f'Bearer {access_token}'
        }

        response = requests.post(url, headers=headers,
                                 data=f"{self.base_url}/{link_to}/{input_id}")

        if response.status_code != 200:
            raise Exception(f"Failed to link biomaterial to process {input_id}. "
                            f"Status code: {response.status_code}, Response: {response.text}")
        else:
            print("linkage successful")

    def transform(self, file):
        """
        Transforms the input file to a JSON object.

        Parameters:
            file (str): The file path.

        Returns:
            dict: The JSON object.
        """
        if self.args.file.endswith('.tsv'):
            json_data = []
            with open(file, 'r', newline='') as file:
                reader = csv.DictReader(file, delimiter='\t')
                for row in reader:
                    json_data.append(row)
            json_data_formatted = {'content': json_data}
            data = json_data_formatted
        elif file.endswith('.csv'):
            df = pd.read_csv(file)
            data = {'content': df.to_dict(orient='records')}
        else:
            with open(file, 'r') as file:
                data = json.load(file)
        return data

    def put_to_provider_api(self, url, access_token):
        """
        Sends a PUT request to the specified URL.

        Parameters:
            url (str): The URL to send the request to.
            access_token (str): Access token for authorization.

        Returns:
            dict: The response data.
        """
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {access_token}'
        }

        response = requests.put(url, headers=headers)
        response_data = response.json()

        return response_data

    def create_child_biomaterial(self, cell_line_entity_id, body, access_token):
        url = self.base_url + '/' + 'biomaterials' + '/' + cell_line_entity_id + '/' + 'childBiomaterials'

        entity_id = self.post_to_provider_api_and_get_entity_id(url, body, access_token)
        return entity_id

    def link_entity_to_envelope(self, type, entity_id, submission_envelope_id, access_token):
        # TODO: handle other types
        global url

        if type == 'biomaterial':
            url = self.submission_envelope_base_url + '/' + submission_envelope_id + '/' + 'biomaterials' + '/' + entity_id
        elif type == 'file':
            url = self.submission_envelope_base_url + '/' + submission_envelope_id + '/' + 'files' + '/' + entity_id

        self.put_to_provider_api(url, access_token)
