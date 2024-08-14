# Import necessary modules/classes from ait.commons.util package
import os
import sys
from datetime import datetime

import numpy as np
import pandas as pd
from ait.commons.util.aws_client import Aws
from ait.commons.util.command.list import CmdList
from ait.commons.util.command.submit import CmdSubmit, get_id_from_url, create_new_submission_envelope
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


def create_expression_alterations(submission_instance, submission_envelope_id, access_token, parsed_data):
    expression_alterations = parsed_data['expression_alterations']
    expression_alterations_df = parsed_data['expression_alterations_df']

    expression_alterations_entity_id_column_name = "Id"

    if expression_alterations_entity_id_column_name not in expression_alterations_df.columns:
        expression_alterations_df[expression_alterations_entity_id_column_name] = np.nan

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
        expression_alterations_df[expression_alterations_entity_id_column_name] = (
            expression_alterations_df[expression_alterations_entity_id_column_name]
            .astype(object))
        expression_alterations_df.loc[
            expression_alterations_df[
                'expression_alteration_id'] == expression_alteration.expression_alteration_id,
            expression_alterations_entity_id_column_name
        ] = expression_alteration_id

    return expression_alterations, expression_alterations_df


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
        self.user_profile = get_profile('morphic-util')
        self.access_token = self.user_profile.access_token
        self.aws = Aws(self.user_profile)
        self.provider_api = APIProvider(self.BASE_URL)
        self.validation_errors = []
        self.submission_errors = []
        self.submission_envelope_id = None

        # Assign and validate required arguments
        self.action = self._get_required_arg('action', "Submission action (ADD, MODIFY or DELETE) is mandatory")
        self.dataset = self._get_required_arg('dataset', (
            "Dataset is mandatory to be registered before submitting dataset metadata. "
            "Please submit your study using the submit option, register your dataset using "
            "the same option, and link your dataset to your study before proceeding with this submission."
        ))

        # Validate file argument only if action is not DELETE
        if self.action != 'DELETE':
            self.file = self._get_required_arg('file', "File is mandatory")
        else:
            print(f"Deleting dataset {self.dataset}")

    def _get_required_arg(self, attr_name, error_message):
        """
        Helper function to get a required argument and print an error message if it's missing.

        Args:
            attr_name (str): The name of the attribute to check in self.args.
            error_message (str): The error message to print if the attribute is missing.

        Returns:
            The value of the attribute if it exists, otherwise None.
        """
        value = getattr(self.args, attr_name, None)
        if value is None:
            print(error_message)
            sys.exit(1)
        return value

    def run(self):
        """
        Execute the command file submission process.
        """
        submission_instance = CmdSubmit(self)

        try:
            if self._is_delete_action():
                return self._handle_delete(submission_instance)

            list_of_files_in_upload_area = self._list_files_in_upload_area()

            if self.file:
                try:
                    self._process_submission(submission_instance, list_of_files_in_upload_area)
                    return True, "SUBMISSION IS SUCCESSFUL."
                except Exception as e:
                    return self.delete_actions(self.submission_envelope_id, submission_instance, e)
        except KeyboardInterrupt:
            # Handle the interruption and exit gracefully
            print("\nProcess interrupted by user. Exiting gracefully...")
            self.delete_actions(self.submission_envelope_id, submission_instance, None)
            sys.exit(0)  # Exit with a zero status code indicating a clean exit
        except Exception as e:
            # Handle any other unexpected exceptions
            print(f"An unexpected error occurred: {str(e)}")
            self.delete_actions(self.submission_envelope_id, submission_instance, None)
            sys.exit(1)  # Exit with a non-zero status code indicating an error

    def _is_delete_action(self):
        """Check if the current action is 'DELETE'."""
        return self.action.lower() == 'delete'

    def _handle_delete(self, submission_instance):
        """Handle the deletion of a dataset."""
        self.file = None
        submission_instance.delete_dataset(self.dataset, self.access_token)
        return True, None

    def _list_files_in_upload_area(self):
        """List files in the upload area."""
        list_instance = CmdList(self.aws, self.args)
        return list_instance.list_bucket_contents_and_return(self.dataset, '')

    def _process_submission(self, submission_instance, list_of_files_in_upload_area):
        """Process the file submission."""
        parser = SpreadsheetSubmitter(self.file)
        parsed_data = self._parse_spreadsheet(parser)
        self._validate_and_upload(parsed_data, submission_instance, list_of_files_in_upload_area)
        # original expression alteration data frame
        expression_alteration_df = parsed_data['expression_alterations_df']
        parent_cell_line_name = parsed_data['parent_cell_line_name']

        # TODO: Handle expression alterations in MODIFY
        if self._is_add_action():
            self._create_submission_envelope(submission_instance)
            print(f"Creating parental cell line with name {parent_cell_line_name}")

            parent_cell_line_id = self._submit_parent_cell_line(submission_instance, parent_cell_line_name)

            print(f"Parental cell line with name {parent_cell_line_name} created with id: {parent_cell_line_id}")

            created_expression_alterations, expression_alteration_df = self._submit_expression_alterations(
                submission_instance, parsed_data)
            self.link_parent_cell_line_expression_alteration(
                submission_instance, self.access_token, parent_cell_line_id, created_expression_alterations
            )

        updated_dfs, message = self._perform_main_submission(submission_instance, parsed_data)

        if message == 'SUCCESS':
            self._save_and_upload_results(updated_dfs, expression_alteration_df)
        else:
            return self.delete_actions(self.submission_envelope_id, submission_instance, None)

    def _parse_spreadsheet(self, parser):
        try:
            """Parse the spreadsheet into different sections."""
            expression_alterations, expression_alterations_df = parser.get_expression_alterations(
                'Expression alteration strategy', self.action, self.validation_errors
            )
            cell_lines, cell_lines_df, parent_cell_line_name = parser.get_cell_lines(
                'Cell line', self.action, self.validation_errors
            )
            differentiated_cell_lines, differentiated_cell_lines_df = parser.get_differentiated_cell_lines(
                'Differentiated cell line', self.action, self.validation_errors
            )
            merge_cell_line_and_differentiated_cell_line(cell_lines, differentiated_cell_lines, self.validation_errors)

            library_preparations, library_preparations_df = parser.get_library_preparations(
                'Library preparation', self.action, self.validation_errors
            )
            merge_differentiated_cell_line_and_library_preparation(differentiated_cell_lines, library_preparations,
                                                                   self.validation_errors)
            sequencing_files, sequencing_files_df = parser.get_sequencing_files(
                'Sequence file', self.action, self.validation_errors
            )
            merge_library_preparation_sequencing_file(library_preparations, sequencing_files, self.validation_errors)

            return {
                "expression_alterations": expression_alterations,
                "expression_alterations_df": expression_alterations_df,
                "cell_lines": cell_lines,
                "cell_lines_df": cell_lines_df,
                "parent_cell_line_name": parent_cell_line_name,
                "differentiated_cell_lines": differentiated_cell_lines,
                "differentiated_cell_lines_df": differentiated_cell_lines_df,
                "library_preparations": library_preparations,
                "library_preparations_df": library_preparations_df,
                "sequencing_files": sequencing_files,
                "sequencing_files_df": sequencing_files_df,
            }
        except Exception:
            self.validation_errors.append(f"Spreadsheet is invalid {self.file}")
            return None

    def _validate_and_upload(self, parsed_data, submission_instance, list_of_files_in_upload_area):
        """Validate the parsed data and upload the file."""
        # validate_sequencing_files(parsed_data['sequencing_files'], list_of_files_in_upload_area, self.dataset)
        try:
            # exit now if there are validation errors in the spreadsheet
            if self.validation_errors:
                raise ValidationError(self.validation_errors)
        except ValidationError as e:
            # Print the error message
            print(e)
            # Exit the program with a non-zero status code to indicate an error
            sys.exit(1)

        print(f"File {self.file} is validated successfully. Initiating submission")
        print(f"File {self.file} being uploaded to storage")

        upload_instance = CmdUpload(self.aws, self.args)
        upload_instance.upload_file(self.dataset, self.file, os.path.basename(self.file))

    def _is_add_action(self):
        """Check if the current action is 'ADD'."""
        return self.action.lower() == 'add'

    def _is_modify_action(self):
        """Check if the current action is 'MODIFY'."""
        return self.action.lower() == 'modify'

    def _create_submission_envelope(self, submission_instance):
        """Create a new submission envelope."""
        submission_envelope_response, status_code = create_new_submission_envelope(
            self.SUBMISSION_ENVELOPE_CREATE_URL, access_token=self.access_token
        )
        if status_code in (200, 201):
            self.submission_envelope_id = get_id_from_url(submission_envelope_response['_links']['self']['href'])
            print(f"Submission envelope for this submission is: {self.submission_envelope_id}")
        else:
            raise Exception(f"Failed to create submission envelope. Status code: {status_code}")

    def _submit_parent_cell_line(self, submission_instance, parent_cell_line_name):
        """Submit the parent cell line."""
        return submission_instance.use_existing_envelope_and_submit_entity(
            'biomaterial', get_content(parent_cell_line_name),
            self.submission_envelope_id, self.access_token
        )

    def _submit_expression_alterations(self, submission_instance, parsed_data):
        """Submit expression alterations."""
        return create_expression_alterations(
            submission_instance, self.submission_envelope_id, self.access_token,
            parsed_data
        )

    def _perform_main_submission(self, submission_instance, parsed_data):
        """Perform the main submission."""
        # Unpack the returned values into a list and the message separately
        updated_dfs, message = submission_instance.multi_type_submission(
            parsed_data['cell_lines'], parsed_data['expression_alterations'], parsed_data['cell_lines_df'],
            parsed_data['differentiated_cell_lines_df'], parsed_data['library_preparations_df'],
            parsed_data['sequencing_files_df'], self.submission_envelope_id,
            self.dataset, self.access_token, self.action, self.submission_errors
        )
        return updated_dfs, message

    def _save_and_upload_results(self, updated_dfs, expression_alteration_df):
        """Save the updated dataframes and upload the results."""
        current_time = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        output_file = f"submission_result_{current_time}.xlsx"
        try:
            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                updated_dfs[0].to_excel(writer, sheet_name='Cell line', index=False)
                updated_dfs[1].to_excel(writer, sheet_name='Differentiated cell line', index=False)
                updated_dfs[2].to_excel(writer, sheet_name='Library preparation', index=False)
                updated_dfs[3].to_excel(writer, sheet_name='Sequence file', index=False)
                expression_alteration_df.to_excel(writer, sheet_name='Expression alteration strategy', index=False)

            if os.path.exists(output_file):
                CmdUpload(self.aws, self.args).upload_file(self.dataset, output_file, os.path.basename(output_file))
                print(f"File {output_file} uploaded successfully.")
            else:
                raise FileNotFoundError(f"The output file {output_file} was not created or cannot be found.")
        except Exception as e:
            print(f"Failed to upload file {output_file}. Error: {e}, Refer dataset {self.dataset} for tracing metadata")

    def delete_actions(self, submission_envelope_id, submission_instance, error=None):
        """Handle actions needed when a submission fails."""
        try:
            if self._is_add_action():
                self._handle_add_action_failure(submission_envelope_id, submission_instance, error)
            elif self._is_modify_action():
                self._handle_modify_action_failure(error)
        except Exception as e:
            print(f"Failed to rollback submission {submission_envelope_id}: {str(e)}")

    def _handle_add_action_failure(self, submission_envelope_id, submission_instance, error):
        """Handle failure during 'ADD' action."""
        print("SUBMISSION has failed, rolling back")
        print("SUBMISSION ERRORS are listed below. Any metadata created will be deleted now, please wait until "
              "the clean-up finishes")
        print("\n".join(self.submission_errors))

        submission_instance.delete_submission(submission_envelope_id, self.access_token, True)
        submission_instance.delete_dataset(self.dataset, self.access_token)

        if error:
            return False, f"An error occurred: {str(error)}"
        else:
            return False, "Submission has failed, rolled back"

    def _handle_modify_action_failure(self, error):
        """Handle failure during 'MODIFY' action."""
        print("SUBMISSION has failed, contact the support team for next actions")
        print("SUBMISSION ERRORS are listed below.")
        print("\n".join(self.submission_errors))

        if error:
            return False, f"An error occurred: {str(error)}"
        else:
            return False, "Submission has failed, rolled back"

    def link_parent_cell_line_expression_alteration(self, submission_instance,
                                                    access_token,
                                                    parent_cell_line_id,
                                                    created_expression_alterations):
        for expression_alteration in created_expression_alterations:
            print(f"Linking parent cell line {parent_cell_line_id} "
                  f"as input to process of {expression_alteration.expression_alteration_id}")
            submission_instance.perform_hal_linkage(
                f"{self.BASE_URL}/biomaterials/{parent_cell_line_id}/inputToProcesses",
                expression_alteration.id, 'processes', access_token
            )
