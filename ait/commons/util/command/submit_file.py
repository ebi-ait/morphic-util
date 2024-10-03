# Import necessary modules/classes from ait.commons.util package
import os
import sys
from datetime import datetime

import numpy as np
import pandas as pd
from ait.commons.util.aws_client import Aws
from ait.commons.util.command.list import CmdList
from ait.commons.util.command.submit import CmdSubmit, get_entity_id_from_hal_link, create_new_submission_envelope
from ait.commons.util.command.upload import CmdUpload
from ait.commons.util.user_profile import get_profile
from ait.commons.util.provider_api_util import APIProvider
from ait.commons.util.spreadsheet_util import SpreadsheetSubmitter, ValidationError, \
    merge_library_preparation_sequencing_file, merge_cell_line_and_differentiated_cell_line, \
    merge_differentiated_cell_line_and_library_preparation, SubmissionError


# Define a class for handling submission of a command file
def validate_sequencing_files(sequencing_files,
                              list_of_files_in_upload_area,
                              dataset,
                              errors):
    for sequencing_file in sequencing_files:
        match_found = False  # Flag to indicate if a match is found

        for file_key in list_of_files_in_upload_area:
            if sequencing_file.file_name == file_key:
                match_found = True
                break  # Exit the inner loop if a match is found

        if not match_found:
            errors.append(
                f"No matching file found for sequencing file: {sequencing_file.file_name} "
                f"in the upload area for the dataset: {dataset}"
            )


def get_content(unique_value):
    return {"content": unique_value}


def _create_expression_alterations(submission_instance,
                                   submission_envelope_id,
                                   access_token,
                                   expression_alterations,
                                   expression_alterations_df):
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

    return expression_alterations


class CmdSubmitFile:
    BASE_URL = 'http://localhost:8080'
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
            "the submit option, and link your dataset to your study before proceeding with this submission."
        ))

        if self.dataset:
            try:
                self.provider_api.get(f"{self.BASE_URL}/datasets/{self.dataset}",
                                      self.access_token)
            except Exception as e:
                print(f"Dataset does not exist {self.dataset}")
                sys.exit(1)

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
                    return self._delete_actions(self.submission_envelope_id, submission_instance, e)
        except KeyboardInterrupt:
            # Handle the interruption and exit gracefully
            print("\nProcess interrupted by user. Exiting gracefully...")
            self._delete_actions(self.submission_envelope_id, submission_instance, None)
            sys.exit(0)  # Exit with a zero status code indicating a clean exit
        except Exception as e:
            # Handle any other unexpected exceptions
            print(f"An unexpected error occurred: {str(e)}")
            self._delete_actions(self.submission_envelope_id, submission_instance, None)
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
        try:
            """Process the file submission."""
            parser = SpreadsheetSubmitter(self.file)
            parsed_data = self._parse_spreadsheet(parser)
            self._validate_and_upload(parsed_data, list_of_files_in_upload_area)

            # Extract parsed data
            expression_alterations = parsed_data['expression_alterations']
            expression_alterations_df = parsed_data['expression_alterations_df']
            parent_cell_line_name = parsed_data['parent_cell_line_name']
            cell_lines = parsed_data['cell_lines']
            cell_lines_df = parsed_data['cell_lines_df']
            differentiated_cell_lines = parsed_data['differentiated_cell_lines']
            differentiated_cell_lines_df = parsed_data['differentiated_cell_lines_df']
            undifferentiated_cell_lines = parsed_data['undifferentiated_cell_lines']
            undifferentiated_cell_lines_df = parsed_data['undifferentiated_cell_lines_df']
            library_preparations = parsed_data['library_preparations']
            library_preparations_df = parsed_data['library_preparations_df']
            sequencing_files = parsed_data['sequencing_files']
            sequencing_files_df = parsed_data['sequencing_files_df']
            differentiated = parsed_data['differentiated']
            cell_line_sheet_name = parsed_data['cell_line_sheet_name']

            if differentiated:
                differentiated_or_undifferentiated_cell_line_sheet_name = parsed_data[
                    'differentiated_cell_line_sheet_name']
            else:
                differentiated_or_undifferentiated_cell_line_sheet_name = parsed_data[
                    'undifferentiated_cell_line_sheet_name']

            # Initialize lists for created entities
            created_expression_alterations = []
            created_cell_lines = []
            created_differentiated_or_undifferentiated_cell_lines = []
            created_library_preparations = []
            created_sequencing_files = []

            if self._is_add_action():
                self._create_submission_envelope()
                parent_cell_line_id = self._handle_parent_cell_line(submission_instance,
                                                                    parent_cell_line_name)
                created_expression_alterations = self._handle_expression_alterations(
                    submission_instance,
                    expression_alterations,
                    expression_alterations_df,
                    parent_cell_line_name,
                    parent_cell_line_id
                )

            if cell_lines and cell_lines_df is not None:
                created_cell_lines = self._create_cell_lines(
                    submission_instance, cell_lines, cell_lines_df, created_expression_alterations)

            if differentiated_cell_lines and differentiated_cell_lines_df is not None:
                created_differentiated_or_undifferentiated_cell_lines = self._create_differentiated_cell_lines(
                    submission_instance, differentiated_cell_lines, differentiated_cell_lines_df)

            if (undifferentiated_cell_lines and undifferentiated_cell_lines_df is not None
                    and not differentiated):
                created_differentiated_or_undifferentiated_cell_lines = self._create_differentiated_cell_lines(
                    submission_instance, undifferentiated_cell_lines, undifferentiated_cell_lines_df)

            if library_preparations and library_preparations_df is not None:
                created_library_preparations = self._create_library_preparations(
                    submission_instance, library_preparations, library_preparations_df)

            if sequencing_files and sequencing_files_df is not None:
                created_sequencing_files = self._create_sequencing_files(
                    submission_instance, sequencing_files, sequencing_files_df)

            updated_dfs, message = self._establish_links(submission_instance,
                                                         created_cell_lines,
                                                         cell_lines_df,
                                                         created_differentiated_or_undifferentiated_cell_lines,
                                                         differentiated_cell_lines_df if differentiated_cell_lines_df is not None else undifferentiated_cell_lines_df,
                                                         created_library_preparations,
                                                         library_preparations_df,
                                                         created_sequencing_files,
                                                         sequencing_files_df)

            if message == 'SUCCESS':
                self._save_and_upload_results(updated_dfs,
                                              expression_alterations_df,
                                              cell_line_sheet_name,
                                              differentiated_or_undifferentiated_cell_line_sheet_name)
            else:
                return self._delete_actions(self.submission_envelope_id,
                                            submission_instance,
                                            None)
        except ValidationError as e:
            print(f"Validation Error: {e.errors}")
            # self._delete_actions(self.submission_envelope_id, submission_instance, e)
            sys.exit(1)
        except SubmissionError as e:
            print(f"Submission Error: {e.errors}")
            self._delete_actions(self.submission_envelope_id, submission_instance, e)
            sys.exit(1)
        except Exception as e:
            print(f"An unexpected error occurred during submission processing: {e}")
            self._delete_actions(self.submission_envelope_id, submission_instance, e)
            raise e  # Re-raise the exception to propagate it upwards

    def _handle_parent_cell_line(self, submission_instance, parent_cell_line_name):
        """Handles the creation of a parent cell line."""
        parent_cell_line_id = None

        if parent_cell_line_name:
            print(f"Creating parental cell line with name {parent_cell_line_name}")
            parent_cell_line_id = self._submit_parent_cell_line(submission_instance, parent_cell_line_name)
            print(f"Parental cell line with name {parent_cell_line_name} created with id: {parent_cell_line_id}")

        return parent_cell_line_id

    def _handle_expression_alterations(self,
                                       submission_instance,
                                       expression_alterations,
                                       expression_alterations_df,
                                       parent_cell_line_name,
                                       parent_cell_line_id):
        """Handles the creation of expression alterations and links them to the parent cell line if needed."""
        created_expression_alterations = []

        if expression_alterations and expression_alterations_df is not None:
            created_expression_alterations = self._submit_expression_alterations(
                submission_instance, expression_alterations, expression_alterations_df
            )

        if created_expression_alterations and parent_cell_line_id:
            self._link_parent_cell_line_expression_alteration(
                submission_instance,
                self.access_token,
                parent_cell_line_name,
                parent_cell_line_id,
                created_expression_alterations
            )

        return created_expression_alterations

    def _parse_spreadsheet(self, parser):
        try:
            # Determine the necessary sheet names
            tab_names = parser.list_sheets()

            cell_line_sheet_name = next(
                (name for name in ["Cell line", "Clonal cell line"] if name in tab_names), None
            )

            differentiated_cell_line_sheet_name = next(
                (name for name in ["Differentiated cell line", "Differentiated product"] if name in tab_names), None
            )

            undifferentiated_cell_line_sheet_name = (
                "Undifferentiated product" if "Undifferentiated product" in tab_names else None
            )

            undifferentiated_cell_lines = []
            undifferentiated_cell_lines_df = None

            differentiated_cell_lines = []
            differentiated_cell_lines_df = None

            differentiated = False

            # Validate the presence of required sheets
            if not cell_line_sheet_name:
                self.validation_errors.append("Spreadsheet must contain a "
                                              "'Cell line' or 'Clonal cell line' sheet.")

            if not (differentiated_cell_line_sheet_name or undifferentiated_cell_line_sheet_name):
                self.validation_errors.append(
                    "Spreadsheet must contain a "
                    "'Differentiated cell line', 'Undifferentiated product', "
                    "or 'Differentiated product' sheet."
                )

            # Parse different sections of the spreadsheet
            expression_alterations, expression_alterations_df = parser.get_expression_alterations(
                'Expression alteration strategy', self.action, self.validation_errors
            )

            cell_lines, cell_lines_df, parent_cell_line_name = parser.get_cell_lines(
                cell_line_sheet_name, self.action, self.validation_errors
            )

            if differentiated_cell_line_sheet_name:
                differentiated_cell_lines, differentiated_cell_lines_df = parser.get_differentiated_cell_lines(
                    differentiated_cell_line_sheet_name, self.action, self.validation_errors
                )

            if undifferentiated_cell_line_sheet_name:
                undifferentiated_cell_lines, undifferentiated_cell_lines_df = parser.get_undifferentiated_cell_lines(
                    undifferentiated_cell_line_sheet_name, self.action, self.validation_errors
                )

            # Check for errors and merge data
            if differentiated_cell_lines and undifferentiated_cell_lines:
                self.validation_errors.append(
                    "A spreadsheet cannot contain rows in both differentiated and undifferentiated cell lines/ products"
                )

            if differentiated_cell_lines:
                differentiated = True
                merge_cell_line_and_differentiated_cell_line(cell_lines, differentiated_cell_lines,
                                                             self.validation_errors)

            if undifferentiated_cell_lines and not differentiated:
                merge_cell_line_and_differentiated_cell_line(cell_lines, undifferentiated_cell_lines,
                                                             self.validation_errors)

            library_preparations, library_preparations_df = parser.get_library_preparations(
                'Library preparation', self.action, self.validation_errors
            )

            if differentiated_cell_lines:
                merge_differentiated_cell_line_and_library_preparation(
                    differentiated_cell_lines, library_preparations, self.validation_errors
                )

            if undifferentiated_cell_lines and not differentiated:
                merge_differentiated_cell_line_and_library_preparation(
                    undifferentiated_cell_lines, library_preparations, self.validation_errors
                )

            sequencing_files, sequencing_files_df = parser.get_sequencing_files(
                'Sequence file', self.action, self.validation_errors
            )

            merge_library_preparation_sequencing_file(library_preparations, sequencing_files, self.validation_errors)

            # Return the parsed data as a dictionary
            return {
                "expression_alterations": expression_alterations,
                "expression_alterations_df": expression_alterations_df,
                "cell_lines": cell_lines,
                "cell_lines_df": cell_lines_df,
                "parent_cell_line_name": parent_cell_line_name,
                "differentiated_cell_lines": differentiated_cell_lines,
                "differentiated_cell_lines_df": differentiated_cell_lines_df,
                "undifferentiated_cell_lines": undifferentiated_cell_lines,
                "undifferentiated_cell_lines_df": undifferentiated_cell_lines_df,
                "library_preparations": library_preparations,
                "library_preparations_df": library_preparations_df,
                "sequencing_files": sequencing_files,
                "sequencing_files_df": sequencing_files_df,
                "differentiated": differentiated,
                "cell_line_sheet_name": cell_line_sheet_name,
                "differentiated_cell_line_sheet_name": differentiated_cell_line_sheet_name,
                "undifferentiated_cell_line_sheet_name": undifferentiated_cell_line_sheet_name
            }
        except Exception:
            self.validation_errors.append(f"Spreadsheet is invalid {self.file}")
            return None

    def _validate_and_upload(self, parsed_data, list_of_files_in_upload_area):
        """
        # Validate the parsed data and upload the file.
        validate_sequencing_files(parsed_data['sequencing_files'], list_of_files_in_upload_area, self.dataset,
                                  self.validation_errors)
        """
        """
           Handle validation errors, including interacting with the user in case of a missing sheet.
           """
        try:
            # Exit now if there are validation errors in the spreadsheet
            if self.validation_errors:
                raise ValidationError(self.validation_errors)
        except ValidationError as e:
            # Check if the error is related to a missing sheet
            missing_sheet_errors = [msg for msg in self.validation_errors if "Missing sheet" in msg]

            if missing_sheet_errors:
                # Extract the sheet name(s) from the errors
                missing_sheets = ', '.join([msg.split("'")[1] for msg in missing_sheet_errors])
                # Ask the user whether to proceed
                user_response = input(
                    f"A required sheet '{missing_sheets}' is missing. Do you want to proceed anyway? (yes/no): ").strip().lower()
                if user_response == 'yes':
                    print("Proceeding with execution...")
                else:
                    print("Execution terminated due to missing required sheet.")
                    sys.exit(1)
            else:
                # Print the error message
                # print(f"Validation Error: {e.errors}")
                # Exit the program with a non-zero status code to indicate an error
                # sys.exit(1)
                raise ValidationError(self.validation_errors)

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

    def _create_submission_envelope(self):
        """Create a new submission envelope."""
        submission_envelope_response, status_code = create_new_submission_envelope(
            self.SUBMISSION_ENVELOPE_CREATE_URL, access_token=self.access_token
        )
        if status_code in (200, 201):
            self.submission_envelope_id = get_entity_id_from_hal_link(
                submission_envelope_response['_links']['self']['href'])
            print(f"Submission envelope for this submission is: {self.submission_envelope_id}")
        else:
            raise SubmissionError(f"Failed to create submission envelope. Status code: {status_code}")

    def _submit_parent_cell_line(self, submission_instance, parent_cell_line_name):
        """Submit the parent cell line."""
        return submission_instance.use_existing_envelope_and_submit_entity(
            'biomaterial', get_content(parent_cell_line_name),
            self.submission_envelope_id, self.access_token
        )

    def _submit_expression_alterations(self,
                                       submission_instance,
                                       expression_alterations,
                                       expression_alterations_df):
        """Submit expression alterations."""
        return _create_expression_alterations(
            submission_instance, self.submission_envelope_id, self.access_token,
            expression_alterations, expression_alterations_df
        )

    def _create_cell_lines(self,
                           submission_instance,
                           cell_lines,
                           cell_lines_df,
                           expression_alterations):
        for cell_line in cell_lines:
            cell_line_entity_id = submission_instance.handle_cell_line(cell_line, expression_alterations, cell_lines_df,
                                                                       self.submission_envelope_id, self.dataset,
                                                                       self.access_token, self.action,
                                                                       self.submission_errors)
            cell_line.id = cell_line_entity_id

        return cell_lines

    def _create_differentiated_cell_lines(self,
                                          submission_instance,
                                          differentiated_cell_lines,
                                          differentiated_cell_lines_df):
        for differentiated_cell_line in differentiated_cell_lines:
            differentiated_cell_line_entity_id = submission_instance.handle_differentiated_cell_line(None,
                                                                                                     differentiated_cell_line,
                                                                                                     differentiated_cell_lines_df,
                                                                                                     self.submission_envelope_id,
                                                                                                     self.dataset,
                                                                                                     self.access_token,
                                                                                                     self.action,
                                                                                                     self.submission_errors)
            differentiated_cell_line.id = differentiated_cell_line_entity_id

        return differentiated_cell_lines

    def _create_library_preparations(self,
                                     submission_instance,
                                     library_preparations,
                                     library_preparations_df):
        for library_preparation in library_preparations:
            library_preparation_entity_id = submission_instance.handle_library_preparation(None,
                                                                                           library_preparation,
                                                                                           library_preparations_df,
                                                                                           self.submission_envelope_id,
                                                                                           self.dataset,
                                                                                           self.access_token,
                                                                                           self.action,
                                                                                           self.submission_errors)
            library_preparation.id = library_preparation_entity_id

        return library_preparations

    def _create_sequencing_files(self,
                                 submission_instance,
                                 sequencing_files,
                                 sequencing_files_df):
        for sequencing_file in sequencing_files:
            sequencing_file_entity_id = submission_instance.handle_sequencing_file(None,
                                                                                   sequencing_file,
                                                                                   sequencing_files_df,
                                                                                   self.submission_envelope_id,
                                                                                   self.dataset,
                                                                                   self.access_token,
                                                                                   self.action,
                                                                                   self.submission_errors)
            sequencing_file.id = sequencing_file_entity_id

        return sequencing_files

    def _establish_links(self,
                         submission_instance,
                         created_cell_lines,
                         cell_lines_df,
                         differentiated_or_undifferentiated_cell_lines,
                         differentiated_or_undifferentiated_cell_lines_df,
                         created_library_preparations,
                         library_preparations_df,
                         created_sequencing_files,
                         sequencing_files_df):
        """Perform the main submission."""
        # Unpack the returned values into a list and the message separately
        updated_dfs, message = submission_instance.establish_links(
            created_cell_lines,
            cell_lines_df,
            differentiated_or_undifferentiated_cell_lines,
            differentiated_or_undifferentiated_cell_lines_df,
            created_library_preparations,
            library_preparations_df,
            created_sequencing_files,
            sequencing_files_df,
            self.submission_envelope_id,
            self.dataset,
            self.access_token,
            self.action,
            self.submission_errors
        )

        return updated_dfs, message

    def _save_and_upload_results(self,
                                 updated_dfs,
                                 expression_alteration_df,
                                 cell_line_sheet_name,
                                 differentiated_or_undifferentiated_cell_line_sheet_name):
        """Save the updated dataframes and upload the results."""
        current_time = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        output_file = f"submission_result_{current_time}.xlsx"
        try:
            # List of updated DataFrames and corresponding sheet names
            dataframes = [
                (updated_dfs[0], cell_line_sheet_name),
                (updated_dfs[1], differentiated_or_undifferentiated_cell_line_sheet_name),
                (updated_dfs[2], 'Library preparation'),
                (updated_dfs[3], 'Sequence file'),
                (expression_alteration_df, 'Expression alteration strategy')
            ]

            # Create the Excel file and write only non-null DataFrames
            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                for df, sheet_name in dataframes:
                    if df is not None:  # Check if the DataFrame is not None
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
            if os.path.exists(output_file):
                CmdUpload(self.aws, self.args).upload_file(self.dataset, output_file, os.path.basename(output_file))
                print(f"File {output_file} uploaded successfully.")
            else:
                raise FileNotFoundError(f"The output file {output_file} was not created or cannot be found.")
        except Exception as e:
            print(f"Failed to upload file {output_file}. Error: {e}, Refer dataset {self.dataset} for tracing metadata")

    def _delete_actions(self, submission_envelope_id, submission_instance, error=None):
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

    def _link_parent_cell_line_expression_alteration(self,
                                                     submission_instance,
                                                     access_token,
                                                     parent_cell_line_name,
                                                     parent_cell_line_id,
                                                     created_expression_alterations):
        for expression_alteration in created_expression_alterations:
            print(f"Linking parent cell line {parent_cell_line_name} "
                  f"as input to process of {expression_alteration.expression_alteration_id}")
            submission_instance.perform_hal_linkage(
                f"{self.BASE_URL}/biomaterials/{parent_cell_line_id}/inputToProcesses",
                expression_alteration.id, 'processes', access_token
            )
