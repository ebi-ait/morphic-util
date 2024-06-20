import pandas as pd
import json
import numpy as np


class MissingMandatoryFieldError(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)


class MissingEntityError(Exception):
    """Custom exception raised when an expected entity is missing."""

    def __init__(self, missing_type, entity_type, missing_id):
        super().__init__(f"Missing {missing_type} for {entity_type} and ID is {missing_id}")
        self.entity_type = entity_type
        self.missing_type = missing_type
        self.missing_id = missing_id


class CellLine:
    def __init__(self, biomaterial_id, description, derived_accession, clone_id, protocol_id, zygosity, cell_type):
        self.biomaterial_id = biomaterial_id
        self.description = description
        self.derived_accession = derived_accession
        self.clone_id = clone_id
        self.protocol_id = protocol_id
        self.zygosity = zygosity
        self.cell_type = cell_type
        self.differentiated_cell_lines = []

    def add_differentiated_cell_line(self, differentiated_cell_line):
        self.differentiated_cell_lines.append(differentiated_cell_line)

    def __repr__(self):
        return json.dumps(self.to_dict(), indent=2)

    def to_dict(self):
        return {
            "content": {
                "biomaterial_id": self.biomaterial_id,
                "description": self.description,
                "derived_accession": self.derived_accession,
                "clone_id": self.clone_id,
                "protocol_id": self.protocol_id,
                "zygosity": self.zygosity,
                "cell_type": self.cell_type
            }
        }


class DifferentiatedCellLine:
    def __init__(self, biomaterial_id, description, input_biomaterial_id, protocol_id, timepoint_value, timepoint_unit,
                 terminally_differentiated, model_system):
        self.biomaterial_id = biomaterial_id
        self.description = description
        self.input_biomaterial_id = input_biomaterial_id
        self.protocol_id = protocol_id
        self.timepoint_value = timepoint_value
        self.timepoint_unit = timepoint_unit
        self.terminally_differentiated = terminally_differentiated
        self.model_system = model_system
        self.library_preparations = []

    def add_library_preparation(self, library_preparation):
        self.library_preparations.append(library_preparation)

    def __repr__(self):
        return json.dumps(self.to_dict(), indent=2)

    def to_dict(self):
        return {
            "content": {
                "biomaterial_id": self.biomaterial_id,
                "description": self.description,
                "input_biomaterial_id": self.input_biomaterial_id,
                "protocol_id": self.protocol_id,
                "timepoint_value": self.timepoint_value,
                "timepoint_unit": self.timepoint_unit,
                "terminally_differentiated": self.terminally_differentiated,
                "model_system": self.model_system
            }
        }


class LibraryPreparation:
    def __init__(self, biomaterial_id, protocol_id, dissociation_protocol_id, differentiated_biomaterial_id,
                 average_fragment_size, input_amount_value, input_amount_unit,
                 final_yield_value, final_yield_unit, concentration_value, concentration_unit,
                 pcr_cycles, pcr_cycles_for_sample_index):
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

    def add_sequencing_file(self, sequencing_file):
        self.sequencing_files.append(sequencing_file)

    def __repr__(self):
        return json.dumps(self.to_dict(), indent=2)

    def to_dict(self):
        # Replace NaN values and out-of-range float values with None
        def convert_to_valid_json_value(obj):
            if isinstance(obj, float):
                if np.isnan(obj) or not np.isfinite(obj):
                    return None
            return obj

        return {
            "content": {
                "biomaterial_id": self.biomaterial_id,
                "protocol_id": self.protocol_id,
                "dissociation_protocol_id": self.dissociation_protocol_id,
                "differentiated_biomaterial_id": self.differentiated_biomaterial_id,
                "average_fragment_size": convert_to_valid_json_value(self.average_fragment_size),
                "input_amount_value": convert_to_valid_json_value(self.input_amount_value),
                "input_amount_unit": self.input_amount_unit,
                "final_yield_value": convert_to_valid_json_value(self.final_yield_value),
                "final_yield_unit": self.final_yield_unit,
                "concentration_value": convert_to_valid_json_value(self.concentration_value),
                "concentration_unit": self.concentration_unit,
                "pcr_cycles": self.pcr_cycles,
                "pcr_cycles_for_sample_index": convert_to_valid_json_value(self.pcr_cycles_for_sample_index)
            }
        }


class SequencingFile:
    def __init__(self, file_name, library_preparation_id, sequencing_protocol_id, read_index, run_id):
        self.file_name = file_name
        self.library_preparation_id = library_preparation_id
        self.sequencing_protocol_id = sequencing_protocol_id
        self.read_index = read_index
        self.run_id = run_id

    def __repr__(self):
        return json.dumps(self.to_dict(), indent=2)

    def to_dict(self):
        return {
            "content": {
                "file_name": self.file_name,
                "library_preparation_id": self.library_preparation_id,
                "sequencing_protocol_id": self.sequencing_protocol_id,
                "read_index": self.read_index,
                "run_id": self.run_id
            }
        }


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

    def parse_cell_lines(self, sheet_name, column_mapping):
        """
        Parses data related to cell lines from a specified sheet in the Excel file.

        Parameters:
        -----------
        sheet_name : str
            The name of the sheet containing cell line data.
        column_mapping : dict
            A dictionary mapping column names in the sheet to expected attribute names.

        Returns:
        --------
        tuple
            A tuple containing:
            - list of CellLine objects parsed from the specified sheet.
            - pd.DataFrame with the parsed data.
        """
        df = pd.read_excel(self.file_path, sheet_name=sheet_name, engine='openpyxl')
        df.columns = df.columns.str.strip()
        df = df.rename(columns=column_mapping)

        # Check if the required column exists
        if 'cell_line.biomaterial_core.biomaterial_id' not in df.columns:
            raise KeyError("The column 'cell_line.biomaterial_core.biomaterial_id' does not exist.")

        # Filter rows where biomaterial_id is not null
        df = df[df['cell_line.biomaterial_core.biomaterial_id'].notna()]

        # Filter column_mapping to include only keys that exist in df.columns
        columns_to_select = [col_mapping_val for col_mapping_key, col_mapping_val in column_mapping.items() if
                             col_mapping_val in df.columns]

        if not columns_to_select:
            raise ValueError("No valid columns found in the column_mapping that exist in the DataFrame.")

        # Select only columns that are present in df
        df = df[columns_to_select]

        # Define columns to check for values starting with 'ABC' or 'XYZ'
        cols_to_check = ['cell_line.biomaterial_core.biomaterial_id']

        # Create a mask to filter rows where any of the specified columns start with 'ABC' or 'XYZ'
        mask = df[cols_to_check].apply(lambda x: ~x.astype(str).str.startswith(
            ('FILL OUT INFORMATION BELOW THIS ROW', 'A unique ID for the biomaterial.',
             'cell_line.biomaterial_core.biomaterial_id'))).all(axis=1)

        # Apply the mask to filter out rows
        df_filtered = df[mask]

        # Check for mandatory fields and create CellLine objects
        # TODO: for all
        cell_lines = []
        for _, row in df_filtered.iterrows():
            biomaterial_id = row['cell_line.biomaterial_core.biomaterial_id']
            derived_accession = row.get('cell_line.derived_cell_line_accession')
            cell_type = row.get('cell_line.type')

            # Check if biomaterial_id is null
            if pd.isnull(biomaterial_id):
                raise MissingMandatoryFieldError("Biomaterial ID cannot be null.")

            # Check if derived_accession and cell_type are present
            if pd.isnull(derived_accession) or pd.isnull(cell_type):
                raise MissingMandatoryFieldError(
                    "Mandatory fields (derived_accession, cell_type) are required. " + biomaterial_id)

            cell_lines.append(
                CellLine(
                    biomaterial_id=biomaterial_id,
                    description=row.get('cell_line.biomaterial_core.biomaterial_description'),
                    derived_accession=derived_accession,
                    clone_id=row.get('cell_line.clone_id'),
                    protocol_id=row.get('gene_expression_alteration_protocol.protocol_core.protocol_id'),
                    zygosity=row.get('cell_line.zygosity'),
                    cell_type=cell_type
                )
            )

        return cell_lines, df_filtered

    def parse_differentiated_cell_lines(self, sheet_name, column_mapping):
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
        df = pd.read_excel(self.file_path, sheet_name=sheet_name, engine='openpyxl')
        df.columns = df.columns.str.strip()
        df = df.rename(columns=column_mapping)

        # Check if the required column exists
        if 'differentiated_cell_line.biomaterial_core.biomaterial_id' not in df.columns:
            raise KeyError("The column 'differentiated_cell_line.biomaterial_core.biomaterial_id' does not exist.")

        # Filter rows where biomaterial_id is not null
        df = df[df['differentiated_cell_line.biomaterial_core.biomaterial_id'].notna()]

        # Filter column_mapping to include only keys that exist in df.columns
        columns_to_select = [col_mapping_val for col_mapping_key, col_mapping_val in column_mapping.items() if
                             col_mapping_val in df.columns]

        if not columns_to_select:
            raise ValueError("No valid columns found in the column_mapping that exist in the DataFrame.")

        # Select only columns that are present in df
        df = df[columns_to_select]

        # Define columns to check for values starting with 'ABC' or 'XYZ'
        cols_to_check = ['differentiated_cell_line.biomaterial_core.biomaterial_id']

        # Create a mask to filter rows where any of the specified columns start with 'ABC' or 'XYZ'
        mask = df[cols_to_check].apply(lambda x: ~x.astype(str).str.startswith(
            ('FILL OUT INFORMATION BELOW THIS ROW', 'A unique ID for the biomaterial.',
             'differentiated_cell_line.biomaterial_core.biomaterial_id'))).all(axis=1)

        # Apply the mask to filter out rows
        df_filtered = df[mask]

        # Create DifferentiatedCellLine objects from filtered DataFrame rows
        differentiated_cell_lines = [
            DifferentiatedCellLine(
                biomaterial_id=row['differentiated_cell_line.biomaterial_core.biomaterial_id'],
                description=row.get('differentiated_cell_line.biomaterial_core.biomaterial_description'),
                input_biomaterial_id=row.get('cell_line.biomaterial_core.biomaterial_id'),
                protocol_id=row.get('differentiation_protocol.protocol_core.protocol_id'),
                timepoint_value=row.get('differentiated_cell_line.timepoint_value'),
                timepoint_unit=row.get('differentiated_cell_line.timepoint_unit.text'),
                terminally_differentiated=row.get('differentiated_cell_line.terminally_differentiated'),
                model_system=row.get('differentiated_cell_line.model_organ.text')
            )
            for _, row in df_filtered.iterrows()
        ]

        return differentiated_cell_lines, df_filtered

    def parse_library_preparations(self, sheet_name, column_mapping):
        """
        Parses data related to library preparations from a specified sheet in the Excel file.

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
        df = pd.read_excel(self.file_path, sheet_name=sheet_name, engine='openpyxl')
        df.columns = df.columns.str.strip()
        df = df.rename(columns=column_mapping)

        # Check if the required column exists
        if 'library_preparation.biomaterial_core.biomaterial_id' not in df.columns:
            raise KeyError("The column 'library_preparation.biomaterial_core.biomaterial_id' "
                           "does not exist.")

        # Filter rows where biomaterial_id is not null
        df = df[df['library_preparation.biomaterial_core.biomaterial_id'].notna()]
        df = df.applymap(lambda x: None if isinstance(x, float) and (np.isnan(x) or not np.isfinite(x)) else x)

        # Filter column_mapping to include only keys that exist in df.columns
        columns_to_select = [col_mapping_val for col_mapping_key, col_mapping_val in column_mapping.items() if
                             col_mapping_val in df.columns]

        if not columns_to_select:
            raise ValueError("No valid columns found in the column_mapping that exist in the DataFrame.")

        # Select only columns that are present in df
        df = df[columns_to_select]

        # Define columns to check for values starting with 'ABC' or 'XYZ'
        cols_to_check = ['library_preparation.biomaterial_core.biomaterial_id']

        # Create a mask to filter rows where any of the specified columns start with 'ABC' or 'XYZ'
        mask = df[cols_to_check].apply(lambda x: ~x.astype(str).str.startswith(
            ('FILL OUT INFORMATION BELOW THIS ROW', 'A unique ID for the biomaterial.',
             'library_preparation.biomaterial_core.biomaterial_id'))).all(axis=1)

        # Apply the mask to filter out rows
        df_filtered = df[mask]

        # Create LibraryPreparation objects from filtered DataFrame rows
        library_preparations = [
            LibraryPreparation(
                biomaterial_id=row['library_preparation.biomaterial_core.biomaterial_id'],
                protocol_id=row.get('library_preparation_protocol.protocol_core.protocol_id'),
                dissociation_protocol_id=row.get('dissociation_protocol.protocol_core.protocol_id'),
                differentiated_biomaterial_id=row.get('differentiated_cell_line.biomaterial_core.biomaterial_id'),
                average_fragment_size=row.get('library_preparation.average_fragment_size'),
                input_amount_value=row.get('library_preparation.input_amount_value'),
                input_amount_unit=row.get('library_preparation.input_amount_unit'),
                final_yield_value=row.get('library_preparation.final_yield_value'),
                final_yield_unit=row.get('library_preparation.final_yield_unit'),
                concentration_value=row.get('library_preparation.concentration_value'),
                concentration_unit=row.get('library_preparation.concentration_unit'),
                pcr_cycles=row.get('library_preparation.pcr_cycles'),
                pcr_cycles_for_sample_index=row.get('library_preparation.pcr_cycles_for_sample_index')
            )
            for _, row in df_filtered.iterrows()
        ]

        return library_preparations, df_filtered

    def parse_sequencing_files(self, sheet_name, column_mapping):
        """
        Parses data related to sequencing files from a specified sheet in the Excel file.

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
        df = pd.read_excel(self.file_path, sheet_name=sheet_name, engine='openpyxl')
        df.columns = df.columns.str.strip()
        df = df.rename(columns=column_mapping)

        # Check if the required column exists
        if 'sequence_file.file_core.file_name' not in df.columns:
            raise KeyError("The column 'sequence_file.file_core.file_name' does not exist.")

        # Filter rows where file_name is not null
        df = df[df['sequence_file.file_core.file_name'].notna()]

        # Select only columns that are not None in the column_mapping
        df = df[[col for col in column_mapping.values() if col is not None]]

        # Define columns to check for values starting with 'ABC' or 'XYZ'
        cols_to_check = ['sequence_file.file_core.file_name']

        # Create a mask to filter rows where any of the specified columns start with 'ABC' or 'XYZ'
        mask = df[cols_to_check].apply(lambda x: ~x.astype(str).str.startswith(
            ('FILL OUT INFORMATION BELOW THIS ROW', 'The name of the file.',
             'Include the file extension in the file name. For example: R1.fastq.gz; codebook.json',
             'sequence_file.file_core.file_name'))).all(axis=1)

        # Apply the mask to filter out rows
        df_filtered = df[mask]

        # Create SequencingFile objects from filtered DataFrame rows
        sequencing_files = [
            SequencingFile(
                file_name=row['sequence_file.file_core.file_name'],
                library_preparation_id=row.get('library_preparation.biomaterial_core.biomaterial_id'),
                sequencing_protocol_id=row.get('sequencing_protocol.protocol_core.protocol_id'),
                read_index=row.get('sequence_file.read_index'),
                run_id=row.get('sequence_file.run_id')
            )
            for _, row in df_filtered.iterrows()
        ]

        return sequencing_files

    def get_cell_lines(self, sheet_name, column_mapping):
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
        cell_lines, cell_lines_df = self.parse_cell_lines(sheet_name, column_mapping)
        return cell_lines, cell_lines_df

    def get_differentiated_cell_lines(self, sheet_name, column_mapping):
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
                                                                                                       column_mapping)
        return differentiated_cell_lines, differentiated_cell_lines_df

    def merge_cell_line_and_differentiated_cell_line(self, cell_lines, differentiated_cell_lines):
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
        cell_line_ids = {cell_line.biomaterial_id for cell_line in cell_lines}
        for differentiated_cell_line in differentiated_cell_lines:
            if differentiated_cell_line.input_biomaterial_id not in cell_line_ids:
                raise MissingEntityError("Cell Line",
                                         "Differentiated cell line",
                                         differentiated_cell_line.biomaterial_id)

        for cell_line in cell_lines:
            for differentiated_cell_line in differentiated_cell_lines:
                if differentiated_cell_line.input_biomaterial_id == cell_line.biomaterial_id:
                    cell_line.add_differentiated_cell_line(differentiated_cell_line)

    def merge_differentiated_cell_line_and_library_preparation(self, differentiated_cell_lines, library_preparations):
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
        differentiated_ids = {diff_cell.biomaterial_id for diff_cell in differentiated_cell_lines}
        for library_preparation in library_preparations:
            if library_preparation.differentiated_biomaterial_id not in differentiated_ids:
                raise MissingEntityError("Differentiated Cell Line",
                                         "Library preparation",
                                         library_preparation.biomaterial_id)

        for differentiated_cell_line in differentiated_cell_lines:
            for library_preparation in library_preparations:
                if library_preparation.differentiated_biomaterial_id == differentiated_cell_line.biomaterial_id:
                    differentiated_cell_line.add_library_preparation(library_preparation)

    def merge_library_preparation_sequencing_file(self, library_preparations, sequencing_files):
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
        library_ids = {lib_prep.biomaterial_id for lib_prep in library_preparations}
        for sequencing_file in sequencing_files:
            if sequencing_file.library_preparation_id not in library_ids:
                raise MissingEntityError("Library preparation",
                                         "Sequencing file",
                                         sequencing_file.file_name)

        for library_preparation in library_preparations:
            for sequencing_file in sequencing_files:
                if sequencing_file.library_preparation_id == library_preparation.biomaterial_id:
                    library_preparation.add_sequencing_file(sequencing_file)

    def get_library_preparations(self, sheet_name, column_mapping):
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
        library_preparations, df_filtered = self.parse_library_preparations(sheet_name, column_mapping)
        return library_preparations, df_filtered

    def get_sequencing_files(self, sheet_name, column_mapping):
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
        sequencing_files = self.parse_sequencing_files(sheet_name, column_mapping)
        return sequencing_files
