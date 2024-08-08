# Import necessary modules/classes from ait.commons.util package
import pandas as pd
from ait.commons.util.aws_client import Aws
from ait.commons.util.command.list import CmdList
from ait.commons.util.command.submit import CmdSubmit, get_id_from_url
from ait.commons.util.user_profile import get_profile
from ait.commons.util.provider_api_util import APIProvider
from ait.commons.util.spreadsheet_util import SpreadsheetSubmitter


# Define a class for handling submission of a command file
def validate_sequencing_files(sequencing_files, list_of_files_in_upload_area, dataset):
    for sequencing_file in sequencing_files:
        match_found = False  # Flag to indicate if a match is found

        for file_key in list_of_files_in_upload_area:
            if sequencing_file.file_name == file_key:
                match_found = True
                break  # Exit the inner loop if a match is found

        if not match_found:
            raise Exception(
                f"No matching file found for sequencing file: {sequencing_file.file_name} "
                f"in the upload area for the dataset: {dataset}"
            )


class CmdSubmitFile:
    base_url = 'https://api.ingest.dev.archive.morphic.bio'
    submission_envelope_create_url = f"{base_url}/submissionEnvelopes/updateSubmissions"
    submission_envelope_base_url = f"{base_url}/submissionEnvelopes"

    def __init__(self, args):
        """
        Initialize CmdSubmitFile instance.

        Args:
            args: Command-line arguments passed to the script.
        """
        self.args = args
        self.access_token = get_profile('morphic-util').access_token
        self.user_profile = get_profile('morphic-util')
        self.aws = Aws(self.user_profile)
        self.provider_api = APIProvider(self.base_url)
        self.errors = []

        if hasattr(self.args, 'action') and self.args.action is not None:
            self.action = self.args.action
        else:
            print("Submission action (ADD, MODIFY or DELETE) is mandatory")
            return

        if hasattr(self.args, 'dataset') and self.args.dataset is not None:
            self.dataset = self.args.dataset
        else:
            print(
                "Dataset is mandatory to be registered before submitting dataset metadata, "
                "We request you to submit your study using the submit option, register your "
                "dataset using the same option and link your dataset to your study "
                "before proceeding with this submission."
            )
            return

        if hasattr(self.args, 'file') and self.args.file is not None:
            self.file = self.args.file
        else:
            if self.action != 'DELETE':
                print("File is mandatory")
                return
            else:
                print(f"Deleting dataset {self.dataset}")

    def run(self):
        """
        Execute the command file submission process.
        """
        submission_instance = CmdSubmit(self)

        if self.action == 'delete' or self.action == 'DELETE':
            self.file = None
            submission_instance.delete_dataset(self.dataset, self.access_token)
            return True, None

        list_instance = CmdList(self.aws, self.args)
        list_of_files_in_upload_area = list_instance.list_bucket_contents_and_return(self.dataset, '')

        if self.file:
            # Initialize SpreadsheetParser with the provided file path
            parser = SpreadsheetSubmitter(self.file)

            # Parse different sections of the spreadsheet using defined column mappings
            cell_lines, cell_lines_df = parser.get_cell_lines('Cell line', self.action, self.errors)
            differentiated_cell_lines, differentiated_cell_lines_df = parser.get_differentiated_cell_lines(
                'Differentiated cell line', self.action)
            parser.merge_cell_line_and_differentiated_cell_line(cell_lines, differentiated_cell_lines, self.errors)
            library_preparations, library_preparations_df = parser.get_library_preparations(
                'Library preparation', self.action)
            parser.merge_differentiated_cell_line_and_library_preparation(differentiated_cell_lines,
                                                                          library_preparations)
            sequencing_files, sequencing_files_df = parser.get_sequencing_files(
                'Sequence file', self.action)

            # validate_sequencing_files(sequencing_files, list_of_files_in_upload_area, self.dataset)

            parser.merge_library_preparation_sequencing_file(library_preparations, sequencing_files)
            submission_envelope_id = None

            if self.action == 'add' or self.action == 'ADD':
                submission_envelope_response, status_code = submission_instance.create_new_submission_envelope(
                    self.submission_envelope_create_url, access_token=self.access_token
                )
                if status_code == 200 or status_code == 201:
                    self_url = submission_envelope_response['_links']['self']['href']
                    submission_envelope_id = get_id_from_url(self_url)
                    print(f"Submission envelope for this submission is: {submission_envelope_id}")
                else:
                    if status_code == 401:
                        message = "Unauthorized, refresh your access token using the config option"
                        return False, message
                    else:
                        return False, f"Encountered failure with {status_code}"
            else:
                submission_envelope_id = None

            # Perform the submission and get the updated dataframes
            try:
                (
                    updated_cell_lines_df, updated_differentiated_cell_lines_df,
                    updated_library_preparations_df, updated_sequencing_files_df,
                    message
                ) = submission_instance.multi_type_submission(
                    cell_lines, cell_lines_df, differentiated_cell_lines_df,
                    library_preparations_df, sequencing_files_df, submission_envelope_id,
                    self.dataset, self.access_token, self.action
                )

                # Save the updated dataframes to a single Excel file with multiple sheets
                if message == 'SUCCESS':
                    output_file = "submission-result.xlsx"
                    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                        updated_cell_lines_df.to_excel(writer, sheet_name='Cell line', index=False)
                        updated_differentiated_cell_lines_df.to_excel(writer, sheet_name='Differentiated cell line',
                                                                      index=False)
                        updated_library_preparations_df.to_excel(writer, sheet_name='Library preparation', index=False)
                        updated_sequencing_files_df.to_excel(writer, sheet_name='Sequence file', index=False)

                    return True, message
                else:
                    print("Submission has failed, rolling back")
                    submission_instance.delete_submission(submission_envelope_id, self.access_token, True)
                    return False, "Submission has failed, rolled back"
            except Exception as e:
                print("Submission has failed, rolling back")
                submission_instance.delete_submission(submission_envelope_id, self.access_token, True)
                return False, f"An error occurred: {str(e)}"
