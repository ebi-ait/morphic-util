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
    # Column mappings for parsing different sections of the spreadsheet
    cellline_column_mapping = {
        "CELL LINE ID (Required)": "cell_line.biomaterial_core.biomaterial_id",
        "CELL LINE DESCRIPTION": "cell_line.biomaterial_core.biomaterial_description",
        "DERIVED FROM CELL LINE NAME (Required)": "cell_line.derived_cell_line_accession",
        "CLONE ID": "cell_line.clone_id",
        "GENE EXPRESSION ALTERATION PROTOCOL ID": "gene_expression_alteration_protocol.protocol_core.protocol_id",
        "ZYGOSITY": "cell_line.zygosity",
        "CELL LINE TYPE (Required)": "cell_line.type",
        "Unnamed: 7": None,
        "Unnamed: 8": None
    }

    differentiated_cellline_column_mapping = {
        "DIFFERENTIATED CELL LINE ID (Required)": "differentiated_cell_line.biomaterial_core.biomaterial_id",
        "DIFFERENTIATED CELL LINE DESCRIPTION": "differentiated_cell_line.biomaterial_core.biomaterial_description",
        "INPUT CELL LINE ID (Required)": "cell_line.biomaterial_core.biomaterial_id",
        "DIFFERENTIATION PROTOCOL ID (Required)": "differentiation_protocol.protocol_core.protocol_id",
        "TIMEPOINT VALUE": "differentiated_cell_line.timepoint_value",
        "TIMEPOINT UNIT": "differentiated_cell_line.timepoint_unit.text",
        "TERMINALLY DIFFERENTIATED": "differentiated_cell_line.terminally_differentiated",
        "FINAL LINEAGE STAGE": "differentiated_cell_line.terminally_differentiated",
        "Model System": "cell_line.model_organ.text",
        "MODEL SYSTEM": "cell_line.model_organ.text",
        "Unnamed: 8": None
    }

    library_preparation_column_mapping = {
        "LIBRARY PREPARATION ID (Required)": "library_preparation.biomaterial_core.biomaterial_id",
        "LIBRARY PREPARATION PROTOCOL ID (Required)": "library_preparation_protocol.protocol_core.protocol_id",
        "DISSOCIATION PROTOCOL ID (Required)": "dissociation_protocol.protocol_core.protocol_id",
        "DIFFERENTIATED CELL LINE ID (Required)": "differentiated_cell_line.biomaterial_core.biomaterial_id",
        "LIBRARY AVERAGE FRAGMENT SIZE": "library_preparation.average_fragment_size",
        "LIBRARY INPUT AMOUNT VALUE": "library_preparation.input_amount_value",
        "LIBRARY INPUT AMOUNT UNIT": "library_preparation.input_amount_unit",
        "LIBRARY FINAL YIELD VALUE": "library_preparation.final_yield_value",
        "LIBRARY FINAL YIELD UNIT": "library_preparation.final_yield_unit",
        "LIBRARY CONCENTRATION VALUE": "library_preparation.concentration_value",
        "LIBRARY CONCENTRATION UNIT": "library_preparation.concentration_unit",
        "LIBRARY PCR CYCLES": "library_preparation.pcr_cycles",
        "LIBRARY PCR CYCLES FOR SAMPLE INDEX": "library_preparation.pcr_cycles_for_sample_index",
        "Unnamed: 14": None  # Adjust index based on your actual column count
    }

    sequencing_file_column_mapping = {
        "FILE NAME (Required)": "sequence_file.file_core.file_name",
        "INPUT LIBRARY PREPARATION ID (Required)": "library_preparation.biomaterial_core.biomaterial_id",
        "SEQUENCING PROTOCOL ID (Required)": "sequencing_protocol.protocol_core.protocol_id",
        "READ INDEX (Required)": "sequence_file.read_index",
        "RUN ID": "sequence_file.run_id",
        "Unnamed: 5": None  # Adjust index based on your actual column count
    }

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
            cell_lines, cell_lines_df = parser.get_cell_lines('Cell line', self.action, self.cellline_column_mapping)
            differentiated_cell_lines, differentiated_cell_lines_df = parser.get_differentiated_cell_lines(
                'Differentiated cell line', self.action, self.differentiated_cellline_column_mapping
            )
            parser.merge_cell_line_and_differentiated_cell_line(cell_lines, differentiated_cell_lines)
            library_preparations, library_preparations_df = parser.get_library_preparations(
                'Library preparation', self.action, self.library_preparation_column_mapping
            )
            parser.merge_differentiated_cell_line_and_library_preparation(differentiated_cell_lines,
                                                                          library_preparations)
            sequencing_files, sequencing_files_df = parser.get_sequencing_files(
                'Sequence file', self.action, self.sequencing_file_column_mapping
            )

            validate_sequencing_files(sequencing_files, list_of_files_in_upload_area, self.dataset)

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
                        message = "Unauthorized, refresh your tokens using the config option"
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
