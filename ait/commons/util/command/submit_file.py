# Import necessary modules/classes from ait.commons.util package
from ait.commons.util.command.submit import CmdSubmit, get_id_from_url
from ait.commons.util.user_profile import get_profile
from ait.commons.util.util.spreadsheet_util import SpreadsheetSubmitter


# Define a class for handling submission of a command file
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

    base_url = 'http://localhost:8080'
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

        if hasattr(self.args, 'file') and self.args.file is not None:
            self.file = self.args.file
        else:
            self.file = None

    def run(self):
        """
        Execute the command file submission process.
        """
        submission_instance = CmdSubmit(self)

        if self.file:
            # Initialize SpreadsheetParser with the provided file path
            parser = SpreadsheetSubmitter(self.file)

            # Parse different sections of the spreadsheet using defined column mappings
            cell_lines = parser.get_cell_lines('Cell line ', self.cellline_column_mapping)
            differentiated_cell_lines = parser.get_differentiated_cell_lines('Differentiated cell line',
                                                                             self.differentiated_cellline_column_mapping)
            parser.merge_cell_line_and_differentiated_cell_line(cell_lines, differentiated_cell_lines)
            library_preparations = parser.get_library_preparations('Library preparation',
                                                                   self.library_preparation_column_mapping)
            parser.merge_differentiated_cell_line_and_library_preparation(differentiated_cell_lines,
                                                                          library_preparations)
            sequencing_files = parser.get_sequencing_files('Sequence file', self.sequencing_file_column_mapping)
            parser.merge_library_preparation_sequencing_file(library_preparations, sequencing_files)

            # Print each CellLine object in CellLineMaster
            submission_envelope_response = submission_instance.create_new_submission_envelope(
                self.submission_envelope_create_url,
                access_token=self.access_token)
            self_url = submission_envelope_response['_links']['self']['href']
            submission_envelope_id = get_id_from_url(self_url)

            print("Submission envelope for this submission is: " + submission_envelope_id)

            submission_instance.multi_type_submission(cell_lines, submission_envelope_id, self.access_token)
