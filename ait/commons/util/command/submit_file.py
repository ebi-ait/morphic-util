# Import necessary modules/classes from ait.commons.util package
import os
import sys

import pandas as pd
from ait.commons.util.aws_client import Aws
from ait.commons.util.command.list import CmdList
from ait.commons.util.command.submit import CmdSubmit, get_id_from_url
from ait.commons.util.command.upload import CmdUpload
from ait.commons.util.user_profile import get_profile
from ait.commons.util.provider_api_util import APIProvider
from ait.commons.util.spreadsheet_util import SpreadsheetSubmitter, ValidationError, \
    merge_library_preparation_sequencing_file, merge_cell_line_and_differentiated_cell_line, \
    merge_differentiated_cell_line_and_library_preparation


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


def get_content(unique_value):
    return {"content": unique_value}


def create_expression_alterations(submission_instance, submission_envelope_id, access_token, expression_alterations):
    for expression_alteration in expression_alterations:
        # Submit the expression alteration and retrieve the ID
        expression_alteration_id = submission_instance.use_existing_envelope_and_submit_entity(
            'process',
            expression_alteration.to_dict(),  # Convert the object to a dictionary for submission
            submission_envelope_id,
            access_token
        )
        # Set the retrieved ID in the ExpressionAlterationStrategy object
        expression_alteration.id = expression_alteration_id

    return expression_alterations


def link_cell_line_parent_cell_line_expression_alretation(submission_instance,
                                                          submission_envelope_id,
                                                          access_token,
                                                          parent_cell_line_id,
                                                          created_expression_alterations,
                                                          cell_lines):
    pass


class CmdSubmitFile:
    BASE_URL = 'https://api.ingest.dev.archive.morphic.bio'
    SUBMISSION_ENVELOPE_CREATE_URL = f"{BASE_URL}/submissionEnvelopes/updateSubmissions"
    SUBMISSION_ENVELOPE_BASE_URL = f"{BASE_URL}/submissionEnvelopes"

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
        self.provider_api = APIProvider(self.BASE_URL)
        self.validation_errors = []
        self.submission_errors = []

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

        if self.action.lower() == 'delete':
            self.file = None
            submission_instance.delete_dataset(self.dataset, self.access_token)
            return True, None

        list_instance = CmdList(self.aws, self.args)
        list_of_files_in_upload_area = list_instance.list_bucket_contents_and_return(self.dataset, '')

        if self.file:
            # Initialize SpreadsheetParser with the provided file path
            parser = SpreadsheetSubmitter(self.file)

            # Parse different sections of the spreadsheet using defined column mappings
            expression_alterations, expression_alterations_df = (parser.get_expression_alterations
                                                                 ('Expression alteration strategy',
                                                                  self.action, self.validation_errors))
            cell_lines, cell_lines_df, parent_cell_line_name = parser.get_cell_lines('Cell line', self.action,
                                                                                     self.validation_errors)
            differentiated_cell_lines, differentiated_cell_lines_df = parser.get_differentiated_cell_lines(
                'Differentiated cell line', self.action, self.validation_errors)
            merge_cell_line_and_differentiated_cell_line(cell_lines, differentiated_cell_lines,
                                                         self.validation_errors)
            library_preparations, library_preparations_df = parser.get_library_preparations(
                'Library preparation', self.action, self.validation_errors)
            merge_differentiated_cell_line_and_library_preparation(differentiated_cell_lines,
                                                                   library_preparations, self.validation_errors)
            sequencing_files, sequencing_files_df = parser.get_sequencing_files(
                'Sequence file', self.action, self.validation_errors)

            # validate_sequencing_files(sequencing_files, list_of_files_in_upload_area, self.dataset)

            merge_library_preparation_sequencing_file(library_preparations, sequencing_files,
                                                      self.validation_errors)
            upload_instance = CmdUpload(self.aws, self.args)

            try:
                if len(self.validation_errors) > 0:
                    raise ValidationError(self.validation_errors)
                else:
                    print(f"File {self.file} is validated successfully. Initiating submission")
                    print(f"File {self.file} being uploaded to storage")
                    upload_instance.upload_file(self.dataset, self.file, os.path.basename(self.file))
            except ValidationError as e:
                print(e)
                sys.exit(1)

            if self.action.lower() == 'add':
                submission_envelope_response, status_code = submission_instance.create_new_submission_envelope(
                    self.SUBMISSION_ENVELOPE_CREATE_URL, access_token=self.access_token
                )
                if status_code in (200, 201):
                    self_url = submission_envelope_response['_links']['self']['href']
                    submission_envelope_id = get_id_from_url(self_url)
                    print(f"Submission envelope for this submission is: {submission_envelope_id}")
                else:
                    if status_code == 401:
                        message = "Unauthorized, refresh your access token using the config option"
                        return False, message
                    else:
                        return False, f"Encountered failure with status code {status_code}"
            else:
                submission_envelope_id = None

            # Perform the submission and get the updated dataframes
            try:
                parent_cell_line_id = (submission_instance.
                                       use_existing_envelope_and_submit_entity('biomaterial',
                                                                               get_content(
                                                                                   parent_cell_line_name),
                                                                               submission_envelope_id,
                                                                               self.access_token))

                created_expression_alterations = create_expression_alterations(submission_instance,
                                                                               submission_envelope_id,
                                                                               self.access_token,
                                                                               expression_alterations)

                link_cell_line_parent_cell_line_expression_alretation(submission_instance,
                                                                      submission_envelope_id,
                                                                      self.access_token,
                                                                      parent_cell_line_id,
                                                                      created_expression_alterations,
                                                                      cell_lines)
                # submission now
                (
                    updated_cell_lines_df, updated_differentiated_cell_lines_df,
                    updated_library_preparations_df, updated_sequencing_files_df,
                    message
                ) = submission_instance.multi_type_submission(
                    cell_lines, cell_lines_df, differentiated_cell_lines_df,
                    library_preparations_df, sequencing_files_df, submission_envelope_id,
                    self.dataset, self.access_token, self.action, self.submission_errors
                )

                # Save the updated dataframes to a single Excel file with multiple sheets
                if message == 'SUCCESS':
                    output_file = "submission-result.xlsx"
                    try:
                        # Write to Excel file
                        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                            updated_cell_lines_df.to_excel(writer, sheet_name='Cell line', index=False)
                            updated_differentiated_cell_lines_df.to_excel(writer, sheet_name='Differentiated cell line',
                                                                          index=False)
                            updated_library_preparations_df.to_excel(writer, sheet_name='Library preparation',
                                                                     index=False)
                            updated_sequencing_files_df.to_excel(writer, sheet_name='Sequence file', index=False)

                        # Confirm file was written and path exists
                        if os.path.exists(output_file):
                            # Attempt file upload
                            upload_instance.upload_file(self.dataset, output_file, os.path.basename(output_file))
                            print(f"File {output_file} uploaded successfully.")
                        else:
                            raise FileNotFoundError(
                                f"The output file {output_file} was not created or cannot be found.")

                    except Exception as e:
                        print(
                            f"Failed to upload file {output_file}. Error: {e}, Refer dataset {self.dataset} for "
                            f"tracing metadata")
                    return True, "SUBMISSION IS SUCCESSFUL."
                else:
                    return self.delete_actions(submission_envelope_id,
                                               submission_instance,
                                               None)
            except Exception as e:
                return self.delete_actions(submission_envelope_id, submission_instance, e)

    def delete_actions(self, submission_envelope_id, submission_instance, e):
        try:
            print("SUBMISSION has failed, rolling back")
            print("SUBMISSION ERRORS are listed below. Any metadata created will be deleted now, please wait until "
                  "the clean-up finishes")
            print("\n".join(self.submission_errors))
            submission_instance.delete_submission(submission_envelope_id, self.access_token, True)
            submission_instance.delete_dataset(self.dataset, self.access_token)

            if e is None:
                return False, "Submission has failed, rolled back"
            else:
                return False, f"An error occurred: {str(e)}"
        except Exception as e:
            print(f"Failed to rollback submission {submission_envelope_id}")
