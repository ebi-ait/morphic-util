import traceback

import pandas as pd
import json
import numpy as np

"""
class MissingMandatoryFieldError(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

"""


class MissingParentEntityError:

    def add_error(self, missing_type, entity_type, missing_id, errors):
        errors.append(f"Missing {missing_type} for {entity_type} and ID is {missing_id}")


class ValidationError(Exception):
    def __init__(self, errors):
        self.errors = errors
        super().__init__(self._format_message())

    def _format_message(self):
        # This method formats the error message that will be displayed when the exception is raised.
        return "Validation errors occurred:\n" + "\n".join(self.errors)


class SubmissionError(Exception):
    """
    Exception raised for errors during submission.
    Includes a list of errors and an optional underlying exception.
    """

    def __init__(self, errors, original_exception=None):
        self.errors = errors
        self.original_exception = original_exception  # Store the original exception
        super().__init__(self._format_message())

    def _format_message(self):
        """
        Format the error message to include both the list of submission errors and details of the original exception.
        """
        message = "Submission errors occurred:\n" + "\n".join(self.errors)
        if self.original_exception:
            message += "\n\nOriginal Exception Details:\n"
            message += f"Type: {type(self.original_exception).__name__}\n"
            message += f"Message: {str(self.original_exception)}\n"
            message += "Stack Trace:\n" + "".join(traceback.format_tb(self.original_exception.__traceback__))
        return message


"""
class OrphanedEntityError(Exception):
    def __init__(self, type, id):
        super().__init__(f"Orphaned entity {type} and ID is {id}")
        self.type = type
        self.id = id

"""


class CellLine:
    def __init__(self,
                 biomaterial_id,
                 description,
                 derived_from_accession,
                 clone_id,
                 protocol_id,
                 zygosity,
                 cell_type,
                 expression_alteration_id,
                 id):
        self.biomaterial_id = biomaterial_id
        self.description = description
        self.derived_from_accession = derived_from_accession
        self.clone_id = clone_id
        self.protocol_id = protocol_id
        self.zygosity = zygosity
        self.cell_type = cell_type
        self.differentiated_cell_lines = []
        self.expression_alteration_id = expression_alteration_id
        self.id = id

    def add_differentiated_cell_line(self, differentiated_cell_line):
        self.differentiated_cell_lines.append(differentiated_cell_line)

    def __repr__(self):
        return json.dumps(self.to_dict(), indent=2)

    def to_dict(self):
        content = {
            "label": self.biomaterial_id,
            "description": self.description,
            "derived_from_cell_line": self.derived_from_accession,
            "zygosity": self.zygosity,
            "type": self.cell_type
        }

        # Only add optional/custom fields if they are provided
        if self.clone_id:
            content["clone_id"] = self.clone_id  # Not in schema, custom field

        if self.protocol_id:
            content["protocol_id"] = self.protocol_id  # Not in schema, custom field

        if self.expression_alteration_id:
            content["expression_alteration_id"] = self.expression_alteration_id  # Not in schema, custom field

        return {
            "content": content
        }


class ExpressionAlterationStrategy:
    def __init__(self,
                 expression_alteration_id,
                 protocol_id,
                 allele_specific,
                 altered_gene_symbols,
                 altered_gene_ids,
                 targeted_genomic_region,
                 expected_alteration_type,
                 sgrna_target,
                 protocol_method_text,
                 altered_locus,
                 guide_sequence,
                 id):
        self.expression_alteration_id = expression_alteration_id
        self.protocol_id = protocol_id
        self.allele_specific = allele_specific
        self.altered_gene_symbols = altered_gene_symbols
        self.altered_gene_ids = altered_gene_ids
        self.targeted_genomic_region = targeted_genomic_region
        self.expected_alteration_type = expected_alteration_type
        self.sgrna_target = sgrna_target
        self.protocol_method_text = protocol_method_text
        self.altered_locus = altered_locus
        self.guide_sequence = guide_sequence
        self.id = id

    def __repr__(self):
        return json.dumps(self.to_dict(), indent=2)

    def to_dict(self):
        return {
            "content": {
                "expression_alteration_label": self.expression_alteration_id,
                "protocol_id": self.protocol_id,
                "allele_specific": self.allele_specific,
                "altered_gene_symbols": self.altered_gene_symbols,
                "altered_gene_ids": self.altered_gene_ids,
                "targeted_genomic_region": self.targeted_genomic_region,
                "expected_alteration_type": self.expected_alteration_type,
                "sgrna_target": self.sgrna_target,
                "protocol_method_text": self.protocol_method_text,
                "altered_locus": self.altered_locus,
                "guide_sequence": self.guide_sequence,
                "id": self.id
            }
        }


class DifferentiatedCellLine:
    def __init__(self,
                 biomaterial_id,
                 description,
                 input_biomaterial_id,
                 protocol_id,
                 timepoint_value,
                 timepoint_unit,
                 terminally_differentiated,
                 model_system,
                 id):
        self.biomaterial_id = biomaterial_id
        self.description = description
        self.input_biomaterial_id = input_biomaterial_id
        self.protocol_id = protocol_id
        self.timepoint_value = timepoint_value
        self.timepoint_unit = timepoint_unit
        self.terminally_differentiated = terminally_differentiated
        self.model_system = model_system
        self.library_preparations = []
        self.id = id

    def add_library_preparation(self, library_preparation):
        self.library_preparations.append(library_preparation)

    def __repr__(self):
        return json.dumps(self.to_dict(), indent=2)

    def to_dict(self):
        content = {
            "label": self.biomaterial_id,
            "description": self.description,
            "timepoint_value": self.timepoint_value,
            "timepoint_unit": self.timepoint_unit,
            "terminally_differentiated": self.terminally_differentiated,
            "model_system": self.model_system
        }

        # Only add optional/custom fields if they are provided
        if self.input_biomaterial_id:
            content["input_biomaterial_id"] = self.input_biomaterial_id  # Not in schema, custom field

        if self.protocol_id:
            content["protocol_id"] = self.protocol_id  # Not in schema, custom field

        return {
            "content": content
        }


class LibraryPreparation:
    def __init__(self,
                 biomaterial_id,
                 protocol_id,
                 dissociation_protocol_id,
                 differentiated_biomaterial_id,
                 average_fragment_size,
                 input_amount_value,
                 input_amount_unit,
                 final_yield_value,
                 final_yield_unit,
                 concentration_value,
                 concentration_unit,
                 pcr_cycles,
                 pcr_cycles_for_sample_index,
                 id):
        self.biomaterial_id = biomaterial_id
        self.protocol_id = protocol_id
        self.dissociation_protocol_id = dissociation_protocol_id
        self.differentiated_biomaterial_id = differentiated_biomaterial_id
        self.average_fragment_size = average_fragment_size
        self.input_amount_value = input_amount_value
        self.input_amount_unit = input_amount_unit
        self.final_yield_value = final_yield_value
        self.final_yield_unit = final_yield_unit
        self.concentration_value = concentration_value
        self.concentration_unit = concentration_unit
        self.pcr_cycles = pcr_cycles
        self.pcr_cycles_for_sample_index = pcr_cycles_for_sample_index
        self.sequencing_files = []
        self.id = id

    def add_sequencing_file(self, sequencing_file):
        self.sequencing_files.append(sequencing_file)

    def __repr__(self):
        return json.dumps(self.to_dict(), indent=2)

    def to_dict(self):
        # Helper function to handle invalid JSON values
        def convert_to_valid_json_value(value):
            if isinstance(value, float) and (np.isnan(value) or not np.isfinite(value)):
                return None
            return value

        content = {
            "label": self.biomaterial_id,
            "average_fragment_size": convert_to_valid_json_value(self.average_fragment_size),
            "input_amount_value": convert_to_valid_json_value(self.input_amount_value),
            "input_amount_unit": self.input_amount_unit,
            "total_yield_value": convert_to_valid_json_value(self.final_yield_value),
            "total_yield_unit": self.final_yield_unit,
            "concentration_value": convert_to_valid_json_value(self.concentration_value),
            "concentration_unit": self.concentration_unit,
            "pcr_cycles": self.pcr_cycles,
            "pcr_cycles_for_sample_index": convert_to_valid_json_value(self.pcr_cycles_for_sample_index)
        }

        # Add optional/custom fields if they are provided
        if self.protocol_id:
            content["protocol_id"] = self.protocol_id  # Not in schema, custom field
        if self.dissociation_protocol_id:
            content["dissociation_protocol_id"] = self.dissociation_protocol_id  # Not in schema, custom field
        if self.differentiated_biomaterial_id:
            content["differentiated_biomaterial_id"] = self.differentiated_biomaterial_id  # Not in schema, custom field

        return {
            "content": content
        }


class EntityType:
    FILE = 'FILE'


class SequencingFile:
    def __init__(self,
                 file_name,
                 extension,
                 read_index,
                 lane_index=None,
                 read_length=None,
                 checksum=None,
                 library_preparation_id=None,
                 sequencing_protocol_id=None,
                 run_id=None,
                 id=None):
        self.file_name = file_name
        self.extension = extension
        self.read_index = read_index
        self.lane_index = lane_index
        self.read_length = read_length
        self.checksum = checksum
        self.library_preparation_id = library_preparation_id  # Custom field
        self.sequencing_protocol_id = sequencing_protocol_id  # Custom field
        self.run_id = run_id  # Custom field
        self.id = id  # Custom field

    def __repr__(self):
        return json.dumps(self.to_dict(), indent=2)

    def to_dict(self):
        # Helper function to handle invalid JSON values
        def convert_to_valid_json_value(value):
            if isinstance(value, float) and (np.isnan(value) or not np.isfinite(value)):
                return None
            return value

        content = {
            "label": self.file_name,
            "extension": self.extension,
            "read_index": self.read_index,
            "lane_index": convert_to_valid_json_value(self.lane_index),
            "read_length": convert_to_valid_json_value(self.read_length),
            "checksum": self.checksum
        }

        # Add optional/custom fields if they are provided
        if self.library_preparation_id:
            content["library_preparation_id"] = self.library_preparation_id  # Not in schema, custom field
        if self.sequencing_protocol_id:
            content["sequencing_protocol_id"] = self.sequencing_protocol_id  # Not in schema, custom field
        if self.run_id:
            content["run_id"] = self.run_id  # Not in schema, custom field

        return {
            "content": content,
            "fileName": self.file_name
        }


def find_orphans(source_entities,
                 target_entities,
                 source_attr,
                 target_attr,
                 source_type,
                 target_type,
                 errors):
    """
    Validates that each source entity has a corresponding target entity.

    Parameters:
        source_entities (list): The list of source entities.
        target_entities (list): The list of target entities.
        source_attr (str): The attribute name in the source entity to compare.
        target_attr (str): The attribute name in the target entity to compare.
        source_type (str): The type name of the source entity (for error messages).
        target_type (str): The type name of the target entity (for error messages).

    Raises:
        OrphanedEntityError: If a source entity doesn't have a corresponding target entity.
    """
    for source_entity in source_entities:
        match_found = False

        for target_entity in target_entities:
            if getattr(target_entity, target_attr) == getattr(source_entity, source_attr):
                match_found = True
                break

        if not match_found:
            errors.append(f"Orphaned entity {source_type} and ID is {getattr(source_entity, source_attr)}")
            # raise OrphanedEntityError(source_type, getattr(source_entity, source_attr))

    # print(f"VALIDATED: All {source_type.lower()}s have corresponding {target_type.lower()}s.")


def merge_library_preparation_sequencing_file(library_preparations,
                                              sequencing_files,
                                              errors):
    """
    Merges library preparations and sequencing files based on their IDs.

    Parameters:
    -----------
    library_preparations : list
        A list of LibraryPreparation objects to be merged.
    sequencing_files : list
        A list of SequencingFile objects to be merged.

    Returns:
    --------
    None

    Raises:
    ------
    MissingEntityError:
        If a sequencing file does not have a corresponding library preparation.
    """
    find_orphans(
        source_entities=library_preparations,
        target_entities=sequencing_files,
        source_attr="biomaterial_id",  # Assuming this is the correct attribute
        target_attr="library_preparation_id",
        source_type="Library Preparation",
        target_type="Sequencing File",
        errors=errors
    )

    missing_parent_entity_error = MissingParentEntityError()
    library_ids = {lib_prep.biomaterial_id for lib_prep in library_preparations}

    for sequencing_file in sequencing_files:
        if sequencing_file.library_preparation_id not in library_ids:
            missing_parent_entity_error.add_error("Library Preparation",
                                                  "Sequencing File",
                                                  sequencing_file.file_name,
                                                  errors)

    for library_preparation in library_preparations:
        for sequencing_file in sequencing_files:
            if sequencing_file.library_preparation_id == library_preparation.biomaterial_id:
                library_preparation.add_sequencing_file(sequencing_file)


def merge_differentiated_cell_line_and_library_preparation(differentiated_cell_lines,
                                                           library_preparations,
                                                           errors):
    """
    Merges differentiated cell lines and library preparations based on their biomaterial IDs.

    Parameters:
    -----------
    differentiated_cell_lines : list
        A list of DifferentiatedCellLine objects to be merged.
    library_preparations : list
        A list of LibraryPreparation objects to be merged.

    Returns:
    --------
    None

    Raises:
    ------
    MissingEntityError:
        If a library preparation does not have a corresponding differentiated cell line.
    """

    find_orphans(
        source_entities=differentiated_cell_lines,
        target_entities=library_preparations,
        source_attr="biomaterial_id",
        target_attr="differentiated_biomaterial_id",
        source_type="Differentiated Cell line",
        target_type="Library Preparation",
        errors=errors
    )

    missing_parent_entity_error = MissingParentEntityError()

    differentiated_ids = {diff_cell.biomaterial_id for diff_cell in differentiated_cell_lines}

    for library_preparation in library_preparations:
        if library_preparation.differentiated_biomaterial_id not in differentiated_ids:
            missing_parent_entity_error.add_error("Differentiated Cell Line",
                                                  "Library Preparation",
                                                  library_preparation.biomaterial_id,
                                                  errors)

    for differentiated_cell_line in differentiated_cell_lines:
        for library_preparation in library_preparations:
            if library_preparation.differentiated_biomaterial_id == differentiated_cell_line.biomaterial_id:
                differentiated_cell_line.add_library_preparation(library_preparation)


def merge_cell_line_and_differentiated_cell_line(cell_lines,
                                                 differentiated_cell_lines,
                                                 errors):
    """
    Merges cell lines and differentiated cell lines based on their biomaterial IDs.

    Parameters:
    -----------
    cell_lines : list
        A list of CellLine objects to be merged.
    differentiated_cell_lines : list
        A list of DifferentiatedCellLine objects to be merged.

    Returns:
    --------
    None

    Raises:
    ------
    MissingEntityError:
        If a differentiated cell line does not have a corresponding cell line.
    """

    find_orphans(
        source_entities=cell_lines,
        target_entities=differentiated_cell_lines,
        source_attr="biomaterial_id",
        target_attr="input_biomaterial_id",
        source_type="Cell line",
        target_type="Differentiated Cell line",
        errors=errors
    )

    missing_parent_entity_error = MissingParentEntityError()
    cell_line_ids = {cell_line.biomaterial_id for cell_line in cell_lines}

    for differentiated_cell_line in differentiated_cell_lines:
        if differentiated_cell_line.input_biomaterial_id not in cell_line_ids:
            missing_parent_entity_error.add_error("Cell Line",
                                                  "Differentiated Cell line",
                                                  differentiated_cell_line.biomaterial_id,
                                                  errors)

    for cell_line in cell_lines:
        for differentiated_cell_line in differentiated_cell_lines:
            if differentiated_cell_line.input_biomaterial_id == cell_line.biomaterial_id:
                cell_line.add_differentiated_cell_line(differentiated_cell_line)


class SpreadsheetSubmitter:
    """
    A class for parsing and processing data from an Excel spreadsheet containing information about
    cell lines, differentiated cell lines, library preparations, and sequencing files.

    Attributes:
    ----------
    file_path : str
        The file path to the Excel spreadsheet.

    Methods:
    -------
    list_sheets()
        Retrieves the names of all sheets present in the Excel file.

    parse_cell_lines(sheet_name, column_mapping)
        Parses data related to cell lines from a specified sheet in the Excel file.

    parse_differentiated_cell_lines(sheet_name, column_mapping)
        Parses data related to differentiated cell lines from a specified sheet in the Excel file.

    parse_library_preparations(sheet_name, column_mapping)
        Parses data related to library preparations from a specified sheet in the Excel file.

    parse_sequencing_files(sheet_name, column_mapping)
        Parses data related to sequencing files from a specified sheet in the Excel file.

    get_cell_lines(sheet_name, column_mapping)
        Retrieves parsed cell lines data from a specified sheet in the Excel file.

    get_differentiated_cell_lines(sheet_name, column_mapping)
        Retrieves parsed differentiated cell lines data from a specified sheet in the Excel file.

    merge_cell_line_and_differentiated_cell_line(cell_lines, differentiated_cell_lines)
        Merges cell lines and differentiated cell lines based on their biomaterial IDs.

    merge_differentiated_cell_line_and_library_preparation(differentiated_cell_lines, library_preparations)
        Merges differentiated cell lines and library preparations based on their biomaterial IDs.

    merge_library_preparation_sequencing_file(library_preparations, sequencing_files)
        Merges library preparations and sequencing files based on their IDs.

    get_library_preparations(sheet_name, column_mapping)
        Retrieves parsed library preparations data from a specified sheet in the Excel file.

    get_sequencing_files(sheet_name, column_mapping)
        Retrieves parsed sequencing files data from a specified sheet in the Excel file.
    """

    def __init__(self, file_path):
        """
        Initializes a SpreadsheetSubmitter instance with the given file path.

        Parameters:
        -----------
        file_path : str
            The file path to the Excel spreadsheet.
        """
        self.file_path = file_path

    def list_sheets(self):
        """
        Retrieves the names of all sheets present in the Excel file.

        Returns:
        --------
        list
            A list of sheet names present in the Excel file.
        """
        xls = pd.ExcelFile(self.file_path, engine='openpyxl')
        return xls.sheet_names

    def input_file_to_data_frames(self, sheet_name, action):
        if action.upper() == 'MODIFY':
            skip_rows = 0
        else:
            skip_rows = 3

        # Load the Excel file to retrieve all sheet names
        with pd.ExcelFile(self.file_path, engine='openpyxl') as xls:
            # Trim spaces from sheet names
            sheet_names = {sheet.strip(): sheet for sheet in xls.sheet_names}

        # Attempt to find the trimmed sheet name in the list
        trimmed_sheet_name = sheet_name.strip()

        if trimmed_sheet_name in sheet_names:
            # Read the sheet using the original sheet name (with spaces if they existed)
            df = pd.read_excel(self.file_path, sheet_name=sheet_names[trimmed_sheet_name], engine='openpyxl',
                               skiprows=skip_rows)
        else:
            raise ValidationError(f"Sheet '{sheet_name}' not found in the spreadsheet.")

        return df

    def parse_cell_lines(self,
                         sheet_name,
                         action,
                         errors):
        """
        Parses data related to cell lines from a specified sheet in the Excel file.

        Parameters:
        -----------
        sheet_name : str
            The name of the sheet containing cell line data.

        Returns:
        --------
        tuple
            A tuple containing:
            - list of CellLine objects parsed from the specified sheet.
            - pd.DataFrame with the parsed data.
        """
        df = self.input_file_to_data_frames(sheet_name=sheet_name, action=action)
        df.columns = df.columns.str.strip()
        parent_cell_line_names = []

        # Check if the required column exists
        if 'cell_line.biomaterial_core.biomaterial_id' not in df.columns:
            errors.append(
                f"The column 'cell_line.biomaterial_core.biomaterial_id' does not exist in the {sheet_name} sheet. "
                f"The rest of the file will not be processed")
            return [], df

        # Filter rows where biomaterial_id is not null
        df = df[df['cell_line.biomaterial_core.biomaterial_id'].notna()]
        # Replace invalid float values with None
        df = df.map(lambda x: None if isinstance(x, float) and (np.isnan(x) or not np.isfinite(x)) else x)
        # Define columns to check for invalid starting values
        cols_to_check = ['cell_line.biomaterial_core.biomaterial_id']
        invalid_start_values = (
            'FILL OUT INFORMATION BELOW THIS ROW', 'A unique ID for the biomaterial.',
            'cell_line.biomaterial_core.biomaterial_id'
        )
        # Filter out rows with invalid starting values
        mask = df[cols_to_check].apply(lambda x: ~x.astype(str).str.startswith(invalid_start_values)).all(axis=1)
        df_filtered = df[mask]
        # Check for a unique value in 'cell_line.derived_cell_line_accession'
        derived_col = 'cell_line.derived_cell_line_accession'

        if derived_col in df_filtered.columns:
            parent_cell_line_names = df_filtered[derived_col].dropna().unique()

            if len(parent_cell_line_names) != 1:
                errors.append(
                    f"The column '{derived_col}' must have the same value across all rows. Found values: {parent_cell_line_names}")

                return [], df

        # Process rows to create CellLine objects
        cell_lines = []

        for _, row in df_filtered.iterrows():
            biomaterial_id = row['cell_line.biomaterial_core.biomaterial_id']
            derived_from_accession = row.get('cell_line.derived_cell_line_accession')
            cell_type = row.get('cell_line.type')
            expression_alteration_id = row.get('expression_alteration_id')

            # Error handling for missing mandatory fields
            if pd.isnull(biomaterial_id):
                errors.append("Biomaterial ID cannot be null in any row of the Cell line sheet.")

            if any(pd.isnull(field) for field in [derived_from_accession, cell_type]):
                errors.append(
                    f"Mandatory fields (derived_accession, cell_type, expression_alteration_id) are required for Cell "
                    f"line entity: {biomaterial_id}")

            cell_lines.append(
                CellLine(
                    biomaterial_id=biomaterial_id,
                    description=row.get('cell_line.biomaterial_core.biomaterial_description'),
                    derived_from_accession=derived_from_accession,
                    clone_id=row.get('cell_line.clone_id'),
                    protocol_id=row.get('gene_expression_alteration_protocol.protocol_core.protocol_id'),
                    zygosity=row.get('cell_line.zygosity'),
                    cell_type=cell_type,
                    expression_alteration_id=expression_alteration_id,
                    id=row.get('Id')
                )
            )

        return cell_lines, df_filtered, parent_cell_line_names[0]

    def parse_differentiated_cell_lines(self,
                                        sheet_name,
                                        action,
                                        errors):
        """
        Parses data related to differentiated cell lines from a specified sheet in the Excel file.

        Parameters:
        -----------
        sheet_name : str
            The name of the sheet containing differentiated cell line data.
        column_mapping : dict
            A dictionary mapping column names in the sheet to expected attribute names.

        Returns:
        --------
        list
            A list of DifferentiatedCellLine objects parsed from the specified sheet.
        """
        df = self.input_file_to_data_frames(sheet_name=sheet_name, action=action)
        df.columns = df.columns.str.strip()
        # df = df.rename(columns=column_mapping)
        # Remove unnamed columns (columns without headers)
        # df = df.loc[:, ~df.columns.str.startswith('Unnamed')]

        # Check if the required column exists
        if 'differentiated_cell_line.biomaterial_core.biomaterial_id' not in df.columns:
            errors.append(f"The column 'differentiated_cell_line.biomaterial_core.biomaterial_id' does not "
                          f"exist in {sheet_name} name. The rest of the file will not be processed")
            return [], df

        # Filter rows where biomaterial_id is not null
        df = df[df['differentiated_cell_line.biomaterial_core.biomaterial_id'].notna()]
        df = df.map(lambda x: None if isinstance(x, float) and (np.isnan(x) or not np.isfinite(x)) else x)
        # Define columns to check for values starting with 'ABC' or 'XYZ'
        cols_to_check = ['differentiated_cell_line.biomaterial_core.biomaterial_id']
        # Create a mask to filter rows where any of the specified columns start with 'ABC' or 'XYZ'
        mask = df[cols_to_check].apply(lambda x: ~x.astype(str).str.startswith(
            ('FILL OUT INFORMATION BELOW THIS ROW', 'A unique ID for the biomaterial.',
             'differentiated_cell_line.biomaterial_core.biomaterial_id'))).all(axis=1)
        # Apply the mask to filter out rows
        df_filtered = df[mask]
        # Check for mandatory fields and create Differentiated CellLine objects
        differentiated_cell_lines = []

        for _, row in df_filtered.iterrows():
            differentiated_biomaterial_id = row['differentiated_cell_line.biomaterial_core.biomaterial_id']
            biomaterial_id = row.get('cell_line.biomaterial_core.biomaterial_id')

            # Check if biomaterial_id is null
            if pd.isnull(differentiated_biomaterial_id):
                errors.append("Differentiated Cell line ID cannot be null in any row of the Differentiated Cell line "
                              "sheet.")
                # raise MissingMandatoryFieldError("Differentiated Cell line ID cannot be null in any row.")

            # Check if derived_accession and cell_type are present
            if pd.isnull(biomaterial_id):
                errors.append(f"Input Cell line ID cannot be null for Differentiated Cell line:  "
                              f"{differentiated_biomaterial_id}")
                """
                raise MissingMandatoryFieldError(
                    "Input Cell line ID cannot be null. " + differentiated_biomaterial_id)
                """

            # Create DifferentiatedCellLine objects from filtered DataFrame rows
            differentiated_cell_lines.append(
                DifferentiatedCellLine(
                    biomaterial_id=differentiated_biomaterial_id,
                    description=row.get('differentiated_cell_line.biomaterial_core.biomaterial_description'),
                    input_biomaterial_id=biomaterial_id,
                    protocol_id=row.get('differentiation_protocol.protocol_core.protocol_id'),
                    timepoint_value=row.get('differentiated_cell_line.timepoint_value'),
                    timepoint_unit=row.get('differentiated_cell_line.timepoint_unit.text'),
                    terminally_differentiated=row.get('differentiated_cell_line.terminally_differentiated'),
                    model_system=row.get('differentiated_cell_line.model_organ.text'),
                    id=row.get('Id')
                )
            )

        return differentiated_cell_lines, df_filtered

    def parse_undifferentiated_cell_lines(self,
                                          sheet_name,
                                          action,
                                          errors):
        """
        Parses data related to differentiated cell lines from a specified sheet in the Excel file.

        Parameters:
        -----------
        sheet_name : str
            The name of the sheet containing differentiated cell line data.
        column_mapping : dict
            A dictionary mapping column names in the sheet to expected attribute names.

        Returns:
        --------
        list
            A list of DifferentiatedCellLine objects parsed from the specified sheet.
        """
        df = self.input_file_to_data_frames(sheet_name=sheet_name, action=action)
        df.columns = df.columns.str.strip()
        # df = df.rename(columns=column_mapping)
        # Remove unnamed columns (columns without headers)
        # df = df.loc[:, ~df.columns.str.startswith('Unnamed')]

        # Check if the required column exists
        if 'differentiated_cell_line.biomaterial_core.biomaterial_id' not in df.columns:
            errors.append(f"The column 'differentiated_cell_line.biomaterial_core.biomaterial_id' does not "
                          f"exist in {sheet_name}. The rest of the file will not be processed")
            return [], df

        # Filter rows where biomaterial_id is not null
        df = df[df['differentiated_cell_line.biomaterial_core.biomaterial_id'].notna()]
        df = df.map(lambda x: None if isinstance(x, float) and (np.isnan(x) or not np.isfinite(x)) else x)
        # Define columns to check for values starting with 'ABC' or 'XYZ'
        cols_to_check = ['differentiated_cell_line.biomaterial_core.biomaterial_id']
        # Create a mask to filter rows where any of the specified columns start with 'ABC' or 'XYZ'
        mask = df[cols_to_check].apply(lambda x: ~x.astype(str).str.startswith(
            ('FILL OUT INFORMATION BELOW THIS ROW', 'A unique ID for the biomaterial.',
             'differentiated_cell_line.biomaterial_core.biomaterial_id'))).all(axis=1)
        # Apply the mask to filter out rows
        df_filtered = df[mask]
        # Check for mandatory fields and create Differentiated CellLine objects
        undifferentiated_cell_lines = []

        for _, row in df_filtered.iterrows():
            differentiated_biomaterial_id = row['differentiated_cell_line.biomaterial_core.biomaterial_id']
            biomaterial_id = row.get('cell_line.biomaterial_core.biomaterial_id')

            # Check if biomaterial_id is null
            if pd.isnull(differentiated_biomaterial_id):
                errors.append("Differentiated Cell line ID cannot be null in any row of the Differentiated Cell line "
                              "sheet.")
                # raise MissingMandatoryFieldError("Differentiated Cell line ID cannot be null in any row.")

            # Check if derived_accession and cell_type are present
            if pd.isnull(biomaterial_id):
                errors.append(f"Input Cell line ID cannot be null for Differentiated Cell line:  "
                              f"{differentiated_biomaterial_id}")
                """
                raise MissingMandatoryFieldError(
                    "Input Cell line ID cannot be null. " + differentiated_biomaterial_id)
                """

            # Create DifferentiatedCellLine objects from filtered DataFrame rows
            undifferentiated_cell_lines.append(
                DifferentiatedCellLine(
                    biomaterial_id=differentiated_biomaterial_id,
                    description=row.get('differentiated_cell_line.biomaterial_core.biomaterial_description'),
                    input_biomaterial_id=biomaterial_id,
                    protocol_id=row.get('differentiation_protocol.protocol_core.protocol_id'),
                    timepoint_value=row.get('differentiated_cell_line.timepoint_value'),
                    timepoint_unit=row.get('differentiated_cell_line.timepoint_unit.text'),
                    terminally_differentiated=row.get('differentiated_cell_line.terminally_differentiated'),
                    model_system=row.get('differentiated_cell_line.model_organ.text'),
                    id=row.get('Id')
                )
            )

        return undifferentiated_cell_lines, df_filtered

    def parse_library_preparations(self,
                                   sheet_name,
                                   action,
                                   errors):
        """
        Parses data related to library preparations from a specified sheet in the Excel file.

        Parameters:
        -----------
        sheet_name : str
            The name of the sheet containing library preparation data.

        Returns:
        --------
        list
            A list of LibraryPreparation objects parsed from the specified sheet.
        """
        df = self.input_file_to_data_frames(sheet_name=sheet_name, action=action)
        df.columns = df.columns.str.strip()
        # df = df.rename(columns=column_mapping)
        # Remove unnamed columns (columns without headers)
        # df = df.loc[:, ~df.columns.str.startswith('Unnamed')]
        # Check if the required column exists
        required_columns = [
            'library_preparation.biomaterial_core.biomaterial_id',
            'dissociation_protocol.protocol_core.protocol_id',
            'differentiated_cell_line.biomaterial_core.biomaterial_id',
            'library_preparation_protocol.protocol_core.protocol_id'
        ]

        for col in required_columns:
            if col not in df.columns:
                errors.append(f"The column '{col}' does not exist in the {sheet_name} sheet. "
                              f"The rest of the file will not be processed")

                return [], df

        # Filter rows where biomaterial_id is not null
        df = df[df['library_preparation.biomaterial_core.biomaterial_id'].notna()]
        df = df.map(lambda x: None if isinstance(x, float) and (np.isnan(x) or not np.isfinite(x)) else x)
        # Define columns to check for values starting with 'ABC' or 'XYZ'
        cols_to_check = ['library_preparation.biomaterial_core.biomaterial_id']
        # Create a mask to filter rows where any of the specified columns start with 'ABC' or 'XYZ'
        mask = df[cols_to_check].apply(lambda x: ~x.astype(str).str.startswith(
            ('FILL OUT INFORMATION BELOW THIS ROW', 'A unique ID for the biomaterial.',
             'library_preparation.biomaterial_core.biomaterial_id'))).all(axis=1)
        # Apply the mask to filter out rows
        df_filtered = df[mask]
        # Check for mandatory fields and create Library Preparation objects
        library_preparations = []

        for _, row in df_filtered.iterrows():
            library_preparation_id = row['library_preparation.biomaterial_core.biomaterial_id']
            dissociation_protocol_id = row.get('dissociation_protocol.protocol_core.protocol_id')
            differentiated_biomaterial_id = row.get('differentiated_cell_line.biomaterial_core.biomaterial_id')
            library_preparation_protocol_id = row.get('library_preparation_protocol.protocol_core.protocol_id')

            # Check if required fields are null
            if pd.isnull(library_preparation_id):
                errors.append("Library Preparation ID cannot be null in any row of the Library Preparation sheet.")
                # raise MissingMandatoryFieldError("Library Preparation ID cannot be null in any row.")
            if pd.isnull(dissociation_protocol_id):
                errors.append("Dissociation Protocol ID cannot be null in any row of the Library Preparation sheet.")
                # raise MissingMandatoryFieldError("Dissociation Protocol ID cannot be null in any row.")
            if pd.isnull(differentiated_biomaterial_id):
                errors.append("Differentiated Cell Line ID cannot be null in any row of the Library Preparation sheet.")
                # raise MissingMandatoryFieldError("Differentiated Cell Line ID cannot be null in any row.")
            if pd.isnull(library_preparation_protocol_id):
                errors.append(
                    "Library Preparation Protocol ID cannot be null in any row of the Library Preparation sheet.")
                # raise MissingMandatoryFieldError("Library Preparation Protocol ID cannot be null in any row.")

            # Create LibraryPreparation objects from filtered DataFrame rows
            library_preparations.append(
                LibraryPreparation(
                    biomaterial_id=library_preparation_id,
                    protocol_id=library_preparation_protocol_id,
                    dissociation_protocol_id=dissociation_protocol_id,
                    differentiated_biomaterial_id=differentiated_biomaterial_id,
                    average_fragment_size=row.get('library_preparation.average_fragment_size'),
                    input_amount_value=row.get('library_preparation.input_amount_value'),
                    input_amount_unit=row.get('library_preparation.input_amount_unit'),
                    final_yield_value=row.get('library_preparation.final_yield_value'),
                    final_yield_unit=row.get('library_preparation.final_yield_unit'),
                    concentration_value=row.get('library_preparation.concentration_value'),
                    concentration_unit=row.get('library_preparation.concentration_unit'),
                    pcr_cycles=row.get('library_preparation.pcr_cycles'),
                    pcr_cycles_for_sample_index=row.get('library_preparation.pcr_cycles_for_sample_index'),
                    id=row.get('Id')
                )
            )

        return library_preparations, df_filtered

    def parse_sequencing_files(self,
                               sheet_name,
                               action,
                               errors):
        """
        Parses data related to sequencing files from a specified sheet in the Excel file.

        Parameters:
        -----------
        sheet_name : str
            The name of the sheet containing sequencing file data.

        Returns:
        --------
        list
            A list of SequencingFile objects parsed from the specified sheet.
        """
        df = self.input_file_to_data_frames(sheet_name=sheet_name, action=action)
        df.columns = df.columns.str.strip()
        # df = df.rename(columns=column_mapping)

        # Remove unnamed columns (columns without headers)
        # df = df.loc[:, ~df.columns.str.startswith('Unnamed')]

        # Check if the required column exists
        required_columns = [
            'sequence_file.file_core.file_name',
            'library_preparation.biomaterial_core.biomaterial_id',
            'sequencing_protocol.protocol_core.protocol_id',
            'sequence_file.read_index'
        ]

        for col in required_columns:
            if col not in df.columns:
                errors.append(f"The column '{col}' does not exist in the {sheet_name} sheet. "
                              f"The rest of the file will not be processed")

                return [], df

        # Filter rows where file_name is not null
        df = df[df['sequence_file.file_core.file_name'].notna()]
        df = df.map(lambda x: None if isinstance(x, float) and (np.isnan(x) or not np.isfinite(x)) else x)
        # Define columns to check for values starting with 'ABC' or 'XYZ'
        cols_to_check = ['sequence_file.file_core.file_name']
        # Create a mask to filter rows where any of the specified columns start with 'ABC' or 'XYZ'
        mask = df[cols_to_check].apply(lambda x: ~x.astype(str).str.startswith(
            ('FILL OUT INFORMATION BELOW THIS ROW', 'The name of the file.',
             'Include the file extension in the file name. For example: R1.fastq.gz; codebook.json',
             'sequence_file.file_core.file_name'))).all(axis=1)
        # Apply the mask to filter out rows
        df_filtered = df[mask]

        # Check for mandatory fields and create Sequencing file objects
        sequencing_files = []

        for _, row in df_filtered.iterrows():
            file_name = row['sequence_file.file_core.file_name']
            library_preparation_id = row.get('library_preparation.biomaterial_core.biomaterial_id')
            sequencing_protocol_id = row.get('sequencing_protocol.protocol_core.protocol_id')
            read_index = row.get('sequence_file.read_index')

            # Check if required fields are null
            if pd.isnull(file_name):
                errors.append("Sequence file name cannot be null in any row of the Sequencing File sheet.")
                # raise MissingMandatoryFieldError("Sequence file name cannot be null in any row.")
            if pd.isnull(library_preparation_id):
                errors.append("Library Preparation ID cannot be null in any row of the Sequencing File sheet..")
                # raise MissingMandatoryFieldError("Library Preparation ID cannot be null in any row.")
            if pd.isnull(sequencing_protocol_id):
                errors.append("Sequencing Protocol ID cannot be null in any row of the Sequencing File sheet..")
                # raise MissingMandatoryFieldError("Sequencing Protocol ID cannot be null in any row.")
            if pd.isnull(read_index):
                errors.append("Read Index cannot be null in any row of the Sequencing File sheet..")
                # raise MissingMandatoryFieldError("Read Index cannot be null in any row.")

            # Create SequencingFile objects from filtered DataFrame rows
            sequencing_files.append(
                SequencingFile(
                    file_name=file_name,
                    extension=None,
                    read_index=read_index,
                    lane_index=None,
                    read_length=None,
                    checksum=None,
                    library_preparation_id=library_preparation_id,
                    sequencing_protocol_id=sequencing_protocol_id,
                    run_id=row.get('sequence_file.run_id'),
                    id=row.get('Id')
                )
            )

        return sequencing_files, df_filtered

    def parse_expression_alteration(self,
                                    sheet_name,
                                    action,
                                    errors):
        """
        Parses data related to expression alterations from a specified sheet in the Excel file.

        Parameters:
        -----------
        sheet_name : str
            The name of the sheet containing expression alterations data.
        action : str
            The action to be performed on the data.
        errors : list
            A list to accumulate error messages.

        Returns:
        --------
        tuple
            A tuple containing:
            - A list of ExpressionAlterationStrategy objects parsed from the specified sheet (if valid)
            - The filtered DataFrame of the parsed data
            - A boolean indicating whether the expression alteration strategy sheet exists and is valid
        """
        # Attempt to parse the input file into a DataFrame
        try:
            df = self.input_file_to_data_frames(sheet_name=sheet_name, action=action)
        except Exception as e:
            errors.append(f"Missing sheet '{sheet_name}': {e}")
            return [], None

        # Strip whitespace from column names
        df.columns = df.columns.str.strip()

        # Check if the required column exists
        required_columns = ['expression_alteration_id']
        missing_columns = [col for col in required_columns if col not in df.columns]

        if missing_columns:
            errors.append(
                f"The following required columns are missing in the Expression Alteration Strategy sheet: {', '.join(missing_columns)}")
            return None, df, False  # Return if required columns are missing

        # Filter rows where 'expression_alteration_id' is not null
        df = df[df['expression_alteration_id'].notna()]
        # Replace invalid float values (e.g., NaN, infinite) with None
        df = df.map(lambda x: None if isinstance(x, float) and (np.isnan(x) or not np.isfinite(x)) else x)

        # Define unwanted patterns to filter out unwanted rows
        unwanted_patterns = (
            'FILL OUT INFORMATION BELOW THIS ROW',
            'A unique ID for the gene expression alteration instance..',
            'ID should have no spaces. For example: JAXPE0001_MEIS1, MSKKI119_MEF2C, NWU_AID'
        )

        # Create a mask to filter out rows with unwanted starting values
        mask = df['expression_alteration_id'].astype(str).str.startswith(unwanted_patterns)
        df_filtered = df[~mask]

        # Initialize the list of ExpressionAlterationStrategy objects
        expression_alterations = []

        for _, row in df_filtered.iterrows():
            expression_alterations.append(
                ExpressionAlterationStrategy(
                    expression_alteration_id=row.get('expression_alteration_id'),
                    protocol_id=row.get('gene_expression_alteration_protocol.protocol_core.protocol_id'),
                    allele_specific=row.get('gene_expression_alteration_protocol.allele_specific'),
                    altered_gene_symbols=row.get('gene_expression_alteration_protocol.altered_gene_symbols'),
                    altered_gene_ids=row.get('gene_expression_alteration_protocol.altered_gene_ids'),
                    targeted_genomic_region=row.get('gene_expression_alteration_protocol.targeted_genomic_region'),
                    expected_alteration_type=row.get('gene_expression_alteration_protocol.expected_alteration_type'),
                    sgrna_target=row.get('gene_expression_alteration_protocol.crispr.sgrna_target'),
                    protocol_method_text=row.get('gene_expression_alteration_protocol.method.text'),
                    altered_locus=None,  # Placeholder if required
                    guide_sequence=None,  # Placeholder if required
                    id=row.get('Id')
                )
            )

        # Return the list of objects, the filtered DataFrame, and a flag indicating success
        return expression_alterations, df_filtered

    def get_cell_lines(self,
                       sheet_name,
                       action,
                       errors):
        """
        Retrieves parsed cell lines data from a specified sheet in the Excel file.

        Parameters:
        -----------
        sheet_name : str
            The name of the sheet containing cell line data.
        column_mapping : dict
            A dictionary mapping column names in the sheet to expected attribute names.

        Returns:
        --------
        list
            A list of CellLine objects parsed from the specified sheet.
        """
        cell_lines, cell_lines_df, parent_cell_line_name = self.parse_cell_lines(sheet_name, action, errors)
        return cell_lines, cell_lines_df, parent_cell_line_name

    def get_differentiated_cell_lines(self,
                                      sheet_name,
                                      action,
                                      errors):
        """
        Retrieves parsed differentiated cell lines data from a specified sheet in the Excel file.

        Parameters:
        -----------
        sheet_name : str
            The name of the sheet containing differentiated cell line data.
        column_mapping : dict
            A dictionary mapping column names in the sheet to expected attribute names.

        Returns:
        --------
        list
            A list of DifferentiatedCellLine objects parsed from the specified sheet.
        """
        differentiated_cell_lines, differentiated_cell_lines_df = self.parse_differentiated_cell_lines(sheet_name,
                                                                                                       action, errors)
        return differentiated_cell_lines, differentiated_cell_lines_df

    def get_undifferentiated_cell_lines(self,
                                        sheet_name,
                                        action,
                                        errors):
        """
        Retrieves parsed differentiated cell lines data from a specified sheet in the Excel file.

        Parameters:
        -----------
        sheet_name : str
            The name of the sheet containing differentiated cell line data.
        column_mapping : dict
            A dictionary mapping column names in the sheet to expected attribute names.

        Returns:
        --------
        list
            A list of DifferentiatedCellLine objects parsed from the specified sheet.
        """
        undifferentiated_cell_lines, undifferentiated_cell_lines_df = self.parse_undifferentiated_cell_lines(sheet_name,
                                                                                                             action,
                                                                                                             errors)
        return undifferentiated_cell_lines, undifferentiated_cell_lines_df

    def get_library_preparations(self,
                                 sheet_name,
                                 action,
                                 errors):
        """
        Retrieves parsed library preparations data from a specified sheet in the Excel file.

        Parameters:
        -----------
        sheet_name : str
            The name of the sheet containing library preparation data.
        column_mapping : dict
            A dictionary mapping column names in the sheet to expected attribute names.

        Returns:
        --------
        list
            A list of LibraryPreparation objects parsed from the specified sheet.
        """
        library_preparations, df_filtered = self.parse_library_preparations(sheet_name,
                                                                            action, errors)
        return library_preparations, df_filtered

    def get_sequencing_files(self,
                             sheet_name,
                             action,
                             errors):
        """
        Retrieves parsed sequencing files data from a specified sheet in the Excel file.

        Parameters:
        -----------
        sheet_name : str
            The name of the sheet containing sequencing file data.
        column_mapping : dict
            A dictionary mapping column names in the sheet to expected attribute names.

        Returns:
        --------
        list
            A list of SequencingFile objects parsed from the specified sheet.
        """
        sequencing_files, df_filtered = self.parse_sequencing_files(sheet_name, action, errors)
        return sequencing_files, df_filtered

    def get_expression_alterations(self,
                                   sheet_name,
                                   action,
                                   errors):
        expression_alterations, df_filtered = self.parse_expression_alteration(sheet_name, action, errors)
        return expression_alterations, df_filtered
