import traceback

import pandas as pd
import json
import numpy as np
import json
import requests

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
                 parental_cell_line_name,
                 clone_id,
                 protocol_id,
                 zygosity,
                 cell_type,
                 treatment_condition,
                 wt_control_status,
                 expression_alteration_id,
                 id,
                 parental_only=False):
        self.biomaterial_id = biomaterial_id
        self.description = description
        self.parental_cell_line_name = parental_cell_line_name
        self.clone_id = clone_id
        self.protocol_id = protocol_id
        self.zygosity = zygosity
        self.cell_type = cell_type
        self.treatment_condition = treatment_condition
        self.wt_control_status = wt_control_status
        self.differentiated_cell_lines = []
        self.expression_alteration_id = expression_alteration_id
        self.id = id
        # New flag: if True, output minimal content (for parental cell lines with no alteration)
        self.parental_only = parental_only

    def add_differentiated_cell_line(self, differentiated_cell_line):
        self.differentiated_cell_lines.append(differentiated_cell_line)

    def __repr__(self):
        return json.dumps(self.to_dict(), indent=2)

    def to_dict(self):
        if self.parental_only:
            # Minimal content for a parental cell line not linked to an alteration protocol.
            return {"content": self.biomaterial_id}
        else:
            content = {
                "label": self.biomaterial_id,  # matches 'label' in schema
                "description": self.description,
                "zygosity": self.zygosity,
                "type": self.cell_type,
                "parental_cell_line_name": self.parental_cell_line_name
            }
            if self.clone_id:
                content["clone_id"] = self.clone_id
            if self.protocol_id:
                content["cell_line_generation_protocol"] = self.protocol_id
            if self.treatment_condition:
                content["treatment_condition"] = self.treatment_condition
            if self.wt_control_status:
                content["wt_control_status"] = self.wt_control_status
            return {"content": content}

    @classmethod
    def from_existing(cls, existing):
        content = existing.get("content", {})

        # The database id you need is either in 'id' or in the self HAL link
        db_id = (
            existing.get("id") or
            get_entity_id_from_hal_link(existing["_links"]["self"]["href"])
        )

        return cls(
            biomaterial_id              = content.get("label"),
            description                 = content.get("description"),
            parental_cell_line_name     = content.get("parental_cell_line_name"),
            clone_id                    = content.get("clone_id"),
            protocol_id                 = content.get("cell_line_generation_protocol"),
            zygosity                    = content.get("zygosity"),
            cell_type                   = content.get("type"),
            treatment_condition         = content.get("treatment_condition"),
            wt_control_status           = content.get("wt_control_status"),
            expression_alteration_id    = None,      # keep setter logic in handle_cell_line
            id                          = db_id,     # <— store the **ObjectId**, not the UUID
            parental_only               = False
        )

class ExpressionAlterationStrategy:
    def __init__(self,
                 expression_alteration_id,
                 parent_protocol_id,
                 method,
                 id=None,
                 allele_specific=None,
                 altered_gene_symbol=None,
                 target_gene_hgnc_id=None,
                 targeted_genomic_region=None,
                 expected_alteration_type=None,
                 editing_strategy=None,
                 altered_locus=None,
                 guide_sequence=None,
                 genes=None):
        self.expression_alteration_id = expression_alteration_id
        self.parent_protocol_id = parent_protocol_id
        self.method = method
        self.id = id

        # Legacy mode
        self.allele_specific = allele_specific
        self.altered_gene_symbol = altered_gene_symbol
        self.target_gene_hgnc_id = target_gene_hgnc_id
        self.targeted_genomic_region = targeted_genomic_region
        self.expected_alteration_type = expected_alteration_type
        self.editing_strategy = editing_strategy
        self.altered_locus = altered_locus
        self.guide_sequence = guide_sequence

        # New pooled-style gene list
        self.genes = genes or []

    def to_dict(self):
        # Prefer pooled-style genes array if present
        if self.genes:
            genes_payload = self.genes
        else:
            genes_payload = [{
                "allele_specific": self.allele_specific,
                "altered_gene_symbol": self.altered_gene_symbol,
                "target_gene_hgnc_id": self.target_gene_hgnc_id,
                "targeted_genomic_region": self.targeted_genomic_region,
                "expected_alteration_type": self.expected_alteration_type,
                "editing_strategy": self.editing_strategy,
                "altered_locus": self.altered_locus,
                "guide_sequence": self.guide_sequence
            }]

        return {
            "content": {
                "expression_alteration_id": self.expression_alteration_id,
                "parent_protocol_id": self.parent_protocol_id,
                "genes": genes_payload,
                "method": self.method
            }
        }

    def __repr__(self):
        return json.dumps(self.to_dict(), indent=2)


class DifferentiatedCellLine:
    def __init__(self,
                 biomaterial_id,  # Maps to 'label'
                 description,
                 cell_line_biomaterial_id,  # Maps to 'clonal_cell_line_label'
                 differentiated_product_protocol_id,
                 undifferentiated_product_protocol_id,
                 terminally_differentiated,
                 model_system,
                 timepoint_value,
                 timepoint_unit,
                 treatment_condition=None,  # New field as per schema
                 wt_control_status=None,  # New field as per schema
                 id=None):  # Optional, custom field
        self.biomaterial_id = biomaterial_id  # This maps to 'label' in the schema
        self.description = description
        self.cell_line_biomaterial_id = cell_line_biomaterial_id  # Maps to 'clonal_cell_line_label'
        self.differentiated_product_protocol_id = differentiated_product_protocol_id
        self.undifferentiated_product_protocol_id = undifferentiated_product_protocol_id
        self.terminally_differentiated = terminally_differentiated
        self.model_system = model_system
        self.timepoint_value = timepoint_value
        self.timepoint_unit = timepoint_unit
        self.treatment_condition = treatment_condition  # Added to match schema
        self.wt_control_status = wt_control_status  # Added to match schema
        self.library_preparations = []
        self.id = id  # Custom field not in the schema

    def add_library_preparation(self, library_preparation):
        self.library_preparations.append(library_preparation)

    def __repr__(self):
        return json.dumps(self.to_dict(), indent=2)

    def to_dict(self):
        content = {
            "label": self.biomaterial_id,
            "description": self.description,
            "clonal_cell_line_id": self.cell_line_biomaterial_id,
            "differentiated_product_protocol_id": self.differentiated_product_protocol_id,
            "undifferentiated_product_protocol_id": self.undifferentiated_product_protocol_id,
            "terminally_differentiated": self.terminally_differentiated,
            "model_system": self.model_system,
            "timepoint_value": self.timepoint_value,
            "timepoint_unit": self.timepoint_unit,
        }

        # Add optional fields only if they are provided
        if self.treatment_condition:
            content["treatment_condition"] = self.treatment_condition

        if self.wt_control_status:
            content["wt_control_status"] = self.wt_control_status

        return {
            "content": content
        }


class LibraryPreparation:
    def __init__(self,
                 biomaterial_id,
                 protocol_id,
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
        # Helper function to handle invalid JSON values (e.g., NaN, infinite)
        def convert_to_valid_json_value(value):
            if isinstance(value, float) and (np.isnan(value) or not np.isfinite(value)):
                return None
            return value

        content = {
            "label": self.biomaterial_id,
            "library_preparation_protocol_id": self.protocol_id,
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
        if self.differentiated_biomaterial_id:
            content["differentiated_biomaterial_id"] = self.differentiated_biomaterial_id

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
    For parental cell lines, a target is considered a match if it starts with the source value.

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
        source_value = getattr(source_entity, source_attr)

        for target_entity in target_entities:
            target_value = getattr(target_entity, target_attr)

            if isinstance(target_value, list):
                if source_value in target_value:
                    match_found = True
                    break
            else:
                # For parental cell lines, allow prefix matching.
                if source_type == "Cell line (Parental)":
                    if str(target_value).startswith(str(source_value)):
                        match_found = True
                        break
                else:
                    if target_value == source_value:
                        match_found = True
                        break

        if not match_found:
            errors.append(f"Orphaned entity {source_type} and ID is {source_value}")
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


def merge_differentiated_cell_line_and_library_preparation(differentiated_cell_lines, library_preparations, errors, cell_lines=None):
    """
    Merges differentiated cell lines and library preparations based on their biomaterial IDs.
    An extra optional parameter 'cell_lines' is accepted to avoid unexpected keyword argument errors.
    """
    try:
        find_orphans(
            source_entities=differentiated_cell_lines,
            target_entities=library_preparations,
            source_attr="biomaterial_id",
            target_attr="differentiated_biomaterial_id",
            source_type="Differentiated Cell Line",
            target_type="Library Preparation",
            errors=errors
        )

        missing_parent_entity_error = MissingParentEntityError()
        differentiated_ids = {diff_cell.biomaterial_id for diff_cell in differentiated_cell_lines}

        for library_preparation in library_preparations:
            diff_biomaterial_id = library_preparation.differentiated_biomaterial_id

            if isinstance(diff_biomaterial_id, list):
                missing_ids = [id_ for id_ in diff_biomaterial_id if id_ not in differentiated_ids]
                if missing_ids:
                    missing_parent_entity_error.add_error("Differentiated Cell Line", "Library Preparation", ", ".join(missing_ids), errors)
            else:
                if diff_biomaterial_id not in differentiated_ids:
                    missing_parent_entity_error.add_error("Differentiated Cell Line", "Library Preparation", diff_biomaterial_id, errors)

        for diff_cell in differentiated_cell_lines:
            for library_preparation in library_preparations:
                diff_biomaterial_id = library_preparation.differentiated_biomaterial_id

                if isinstance(diff_biomaterial_id, list):
                    if diff_cell.biomaterial_id in diff_biomaterial_id:
                        diff_cell.add_library_preparation(library_preparation)
                elif diff_biomaterial_id == diff_cell.biomaterial_id:
                    diff_cell.add_library_preparation(library_preparation)

    except Exception as e:
        print(f"Exception occurred during merging of differentiated cell lines and library preparations: {e}")


def merge_cell_line_and_differentiated_cell_line(cell_lines, differentiated_cell_lines, errors, context=None):
    """
    Merges cell lines and differentiated cell lines based on their biomaterial IDs.
    Only parental cell lines (those with clone_id is None) are used for linking.
    For parental cell lines, a prefix match is used.
    """
    # Filter to include only parental cell lines.
    if context == "unperturbed_multiple":
        parental_cell_lines = [cl for cl in cell_lines if cl.clone_id is None]
    else:
        parental_cell_lines = cell_lines
    try:
        find_orphans(
            source_entities=parental_cell_lines,
            target_entities=differentiated_cell_lines,
            source_attr="biomaterial_id",
            target_attr="cell_line_biomaterial_id",
            source_type="Cell line (Parental)",
            target_type="Differentiated Cell line",
            errors=errors
        )

        missing_parent_entity_error = MissingParentEntityError()

        parental_ids = {cl.biomaterial_id for cl in parental_cell_lines}
        for diff_cell in differentiated_cell_lines:
            if diff_cell.cell_line_biomaterial_id not in parental_ids:
                missing_parent_entity_error.add_error("Cell Line", "Differentiated Cell line", diff_cell.cell_line_biomaterial_id, errors)

        for cl in parental_cell_lines:
            for diff_cell in differentiated_cell_lines:
                if diff_cell.cell_line_biomaterial_id == cl.biomaterial_id:
                    cl.add_differentiated_cell_line(diff_cell)

    except Exception as e:
        print(f"Exception occurred during merging: {e}")


def target_in_ids(target, id_set):
    """
    Returns True if the target (which may be a string or a list of strings)
    has any element in id_set.
    """
    if isinstance(target, list):
        return any(item in id_set for item in target)
    else:
        return target in id_set


def merge_differentiated_cell_line_and_library_preparation_for_lp(differentiated_cell_lines, library_preps, errors, cell_lines=None):
    """
    Merges library preparations with differentiated cell lines.
    Only processes library preparations whose differentiated_biomaterial_id is found in differentiated_cell_lines.
    """
    # Create a set of differentiated cell line IDs (these should be strings)
    diff_ids = {d.biomaterial_id for d in differentiated_cell_lines}
    # Use target_in_ids() to allow lp.differentiated_biomaterial_id to be a list or a string.
    library_preps_for_diff = [lp for lp in library_preps if target_in_ids(lp.differentiated_biomaterial_id, diff_ids)]

    if not library_preps_for_diff:
        return  # Nothing to merge for differentiated cell lines

    try:
        find_orphans(
            source_entities=differentiated_cell_lines,
            target_entities=library_preps_for_diff,
            source_attr="biomaterial_id",
            target_attr="differentiated_biomaterial_id",
            source_type="Differentiated Cell Line",
            target_type="Library Preparation",
            errors=errors
        )

        missing_parent_entity_error = MissingParentEntityError()
        for lp in library_preps_for_diff:
            # We check using the helper to avoid errors if lp.differentiated_biomaterial_id is a list.
            if not target_in_ids(lp.differentiated_biomaterial_id, diff_ids):
                missing_parent_entity_error.add_error("Differentiated Cell Line", "Library Preparation", str(lp.differentiated_biomaterial_id), errors)

        for diff_cell in differentiated_cell_lines:
            for lp in library_preps_for_diff:
                # If the target is a list, check if the diff_cell's id is in that list.
                if isinstance(lp.differentiated_biomaterial_id, list):
                    if diff_cell.biomaterial_id in lp.differentiated_biomaterial_id:
                        diff_cell.add_library_preparation(lp)
                elif lp.differentiated_biomaterial_id == diff_cell.biomaterial_id:
                    diff_cell.add_library_preparation(lp)
    except Exception as e:
        print(f"Exception during merging of differentiated cell lines and library preparations: {e}")


def merge_cell_line_and_library_preparation_for_lp(cell_lines, library_preps, errors):
    """
    Merges library preparations with clonal cell lines.
    Only processes library preparations whose differentiated_biomaterial_id is found among clonal cell lines.
    """
    # Build a set of clonal cell line IDs (those with non-null clone_id)
    clonal_ids = {cl.biomaterial_id for cl in cell_lines if cl.clone_id is not None}
    library_preps_for_clones = [lp for lp in library_preps if target_in_ids(lp.differentiated_biomaterial_id, clonal_ids)]

    if not library_preps_for_clones:
        return  # Nothing to merge for clonal cell lines

    try:
        find_orphans(
            source_entities=cell_lines,
            target_entities=library_preps_for_clones,
            source_attr="biomaterial_id",
            target_attr="differentiated_biomaterial_id",
            source_type="Cell Line (Clonal)",
            target_type="Library Preparation",
            errors=errors
        )

        missing_parent_entity_error = MissingParentEntityError()
        for lp in library_preps_for_clones:
            if not target_in_ids(lp.differentiated_biomaterial_id, clonal_ids):
                missing_parent_entity_error.add_error("Cell Line", "Library Preparation", str(lp.differentiated_biomaterial_id), errors)

        for cl in cell_lines:
            if cl.clone_id is not None:
                for lp in library_preps_for_clones:
                    if isinstance(lp.differentiated_biomaterial_id, list):
                        if cl.biomaterial_id in lp.differentiated_biomaterial_id:
                            cl.add_library_preparation(lp)
                    elif lp.differentiated_biomaterial_id == cl.biomaterial_id:
                        cl.add_library_preparation(lp)
    except Exception as e:
        print(f"Exception during merging of clonal cell lines and library preparations: {e}")


def process_library_preparations(cell_lines, differentiated_cell_lines, library_preps, errors):
    """
    For UCSF ingestion, process library preparations only for differentiated cell lines.
    Linking for clonal cell lines is deferred to the submission linking phase.
    """
    # Process only the library preparations for differentiated (parental) cell lines.
    diff_ids = {d.biomaterial_id for d in differentiated_cell_lines}
    library_preps_for_diff = [lp for lp in library_preps if target_in_ids(lp.differentiated_biomaterial_id, diff_ids)]
    if library_preps_for_diff:
        merge_differentiated_cell_line_and_library_preparation_for_lp(differentiated_cell_lines, library_preps_for_diff, errors)

def find_existing_biomaterial_by_label(label, ingest_api_base):
    url = f"{ingest_api_base}/biomaterials/search/findByContentLabel?label={label}"
    print(f"Find_existing_biomaterial_by_label URL '{url}'")
    response = requests.get(url)
    print(f"Find_existing_biomaterial_by_label response '{response}'")
    if response.status_code == 200:
        results = response.json()
        biomaterials = results.get("_embedded", {}).get("biomaterials", [])
        return biomaterials[0] if biomaterials else None
    return None

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
        Retrieves the names of all sheets present in the Excel file,
        trimming any leading or trailing spaces.

        Returns:
        --------
        list
            A list of trimmed sheet names present in the Excel file.
        """
        xls = pd.ExcelFile(self.file_path, engine='openpyxl')
        return [sheet_name.strip() for sheet_name in xls.sheet_names]

    def input_file_to_data_frames(self, sheet_name, action):
        if action and action.upper() == 'MODIFY':
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

    def parse_cell_lines(self, sheet_name, action, errors, context=None):
        """
        Parses cell lines from the clonal cell line sheet.

        In UCSF datasets, each row represents a clone (e.g. iPSC_Rep1) that has an associated
        parental cell line name (e.g. KOLF2.2J_AAVS1_inducible_CRISPRi). Since clones go directly
        to library preparation and the parental cell line is used for differentiation, this function
        creates a separate parental cell line entity if its label is not found among the clones.

        Returns:
            combined (list): A list of CellLine objects including both clones and auto-generated parental cell lines.
            df_filtered (pd.DataFrame): The filtered DataFrame.
            parental_names (list): A list of unique parental cell line names extracted from the sheet.
        """
        df = self.input_file_to_data_frames(sheet_name=sheet_name, action=action)
        df.columns = df.columns.str.strip()
        if 'clonal_cell_line.label' not in df.columns:
            errors.append(f"The column 'clonal_cell_line.label' does not exist in the {sheet_name} sheet.")
            return [], df, []

        # Filter out placeholder rows.
        df = df[df['clonal_cell_line.label'].notna()]
        df = df.map(lambda x: None if isinstance(x, float) and (np.isnan(x) or not np.isfinite(x)) else x)
        mask = df['clonal_cell_line.label'].astype(str).str.startswith('FILL OUT INFORMATION BELOW THIS ROW')
        df_filtered = df[~mask]

        cell_lines = []
        parental_names = set()
        for _, row in df_filtered.iterrows():
            label = row['clonal_cell_line.label']
            parent_name = row.get('clonal_cell_line.parental_cell_line_name')

            print(f"Examining clonal cell line '{label}'")
            existing = find_existing_biomaterial_by_label(label, ingest_api_base="https://api.ingest.archive.morphic.bio")

            if existing:
                print(f"Reusing existing clonal cell line '{label}'")
                cell_line = CellLine.from_existing(existing)
                # Update expression alteration ID if it's provided in the spreadsheet
                ea_id = row.get('expression_alteration.label')
                if ea_id:
                    cell_line.expression_alteration_id = ea_id
                cell_lines.append(cell_line)
                continue

            cell_lines.append(
                CellLine(
                    biomaterial_id=label,
                    description=row.get('clonal_cell_line.description'),
                    parental_cell_line_name=parent_name,
                    clone_id=row.get('clonal_cell_line.clone_id'),
                    protocol_id=row.get('clonal_cell_line.cell_line_generation_protocol'),
                    zygosity=row.get('clonal_cell_line.zygosity'),
                    cell_type=row.get('clonal_cell_line.type'),
                    treatment_condition=row.get('clonal_cell_line.treatment_condition'),
                    wt_control_status=row.get('clonal_cell_line.wt_control_status'),
                    expression_alteration_id=row.get('expression_alteration.label'),
                    id=row.get('Id')
                )
            )
            if parent_name and parent_name != label:
                parental_names.add(parent_name)

        # Only auto‑generate parental cell lines if we’re in UCSF mode.
        if context == "unperturbed_multiple":
            parental_cell_lines = []
            for parent in parental_names:
                if parent not in {cl.biomaterial_id for cl in cell_lines}:
                    parental_cell_lines.append(
                        CellLine(
                            biomaterial_id=parent,
                            description="Auto-generated parental cell line from clonal cell lines",
                            parental_cell_line_name=None,
                            clone_id=None,
                            protocol_id=None,
                            zygosity=None,
                            cell_type=None,
                            treatment_condition=None,
                            wt_control_status=None,
                            expression_alteration_id=None,
                            id=None,
                            parental_only=True
                        )
                    )
            combined = parental_cell_lines + cell_lines
        else:
            combined = cell_lines  # Legacy mode: use only the clones.

        return combined, df_filtered, list(parental_names)

    def parse_differentiated_cell_lines(self, sheet_name, action, errors):
        """
        Parses data related to differentiated cell lines from a specified sheet in the Excel file.
        Uses the 'clonal_cell_line.parental_cell_line_name' (or falls back to 'clonal_cell_line.label')
        to link differentiated products to the parental cell line.
        """
        df = self.input_file_to_data_frames(sheet_name=sheet_name, action=action)
        df.columns = df.columns.str.strip()

        if 'differentiated_product.label' not in df.columns:
            errors.append(f"The column 'differentiated_product.label' does not exist in {sheet_name}. The rest of the file will not be processed")
            return [], df

        df = df[df['differentiated_product.label'].notna()]
        df = df.map(lambda x: None if isinstance(x, float) and (np.isnan(x) or not np.isfinite(x)) else x)
        cols_to_check = ['differentiated_product.label']
        mask = df[cols_to_check].apply(lambda x: ~x.astype(str).str.startswith(
            ('FILL OUT INFORMATION BELOW THIS ROW', 'A unique ID for the biomaterial.',
             'differentiated_cell_line.biomaterial_core.biomaterial_id'))).all(axis=1)
        df_filtered = df[mask]

        differentiated_cell_lines = []
        for _, row in df_filtered.iterrows():
            label = row['differentiated_product.label']
            # Attempt to get the parental cell line name; if missing, fallback to the provided clonal label.
            parent_biomaterial_id = row.get('clonal_cell_line.parental_cell_line_name') or row.get('clonal_cell_line.label')
            if pd.isnull(label):
                errors.append("Differentiated Cell line ID cannot be null in any row of the Differentiated Cell line sheet.")
            if pd.isnull(parent_biomaterial_id):
                errors.append(f"Parental Cell line ID cannot be null for Differentiated Cell line: {label}")

            differentiated_cell_lines.append(
                DifferentiatedCellLine(
                    biomaterial_id=label,
                    description=row.get('differentiated_product.description'),
                    cell_line_biomaterial_id=parent_biomaterial_id,  # Linking to parental cell line
                    differentiated_product_protocol_id=row.get('differentiated_product.differentiated_product_protocol_id'),
                    undifferentiated_product_protocol_id=None,
                    treatment_condition=row.get('differentiated_product.treatment_condition'),
                    wt_control_status=row.get('differentiated_product.wt_control_status'),
                    timepoint_value=row.get('differentiated_product.timepoint_value'),
                    timepoint_unit=row.get('differentiated_product.timepoint_unit'),
                    terminally_differentiated=row.get('differentiated_product.final_timepoint'),
                    model_system=row.get('differentiated_product.model_system'),
                    id=row.get('Id')
                )
            )

        return differentiated_cell_lines, df_filtered

    # TODO: review
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
        if 'undifferentiated_product.label' not in df.columns:
            errors.append(f"The column 'undifferentiated_product.label' does not "
                          f"exist in {sheet_name} name. The rest of the file will not be processed")
            return [], df

        # Filter rows where biomaterial_id is not null
        df = df[df['undifferentiated_product.label'].notna()]
        df = df.map(lambda x: None if isinstance(x, float) and (np.isnan(x) or not np.isfinite(x)) else x)
        # Define columns to check for values starting with 'ABC' or 'XYZ'
        cols_to_check = ['undifferentiated_product.label']
        # Create a mask to filter rows where any of the specified columns start with 'ABC' or 'XYZ'
        mask = df[cols_to_check].apply(lambda x: ~x.astype(str).str.startswith(
            ('FILL OUT INFORMATION BELOW THIS ROW', 'A unique ID for the biomaterial.',
             'differentiated_cell_line.biomaterial_core.biomaterial_id'))).all(axis=1)
        # Apply the mask to filter out rows
        df_filtered = df[mask]
        # Check for mandatory fields and create Differentiated CellLine objects
        undifferentiated_cell_lines = []

        for _, row in df_filtered.iterrows():
            label = row['undifferentiated_product.label']
            parent_biomaterial_id = row.get('clonal_cell_line.label')

            # Check if biomaterial_id is null
            if pd.isnull(label):
                errors.append(
                    "Undifferentiated Cell line ID cannot be null in any row of the Undifferentiated Cell line "
                    "sheet.")
                # raise MissingMandatoryFieldError("Differentiated Cell line ID cannot be null in any row.")

            # Check if derived_accession and cell_type are present
            if pd.isnull(parent_biomaterial_id):
                errors.append(f"Input Cell line ID cannot be null for Undifferentiated Cell line:  "
                              f"{label}")
                """
                raise MissingMandatoryFieldError(
                    "Input Cell line ID cannot be null. " + differentiated_biomaterial_id)
                """

            # Create DifferentiatedCellLine objects from filtered DataFrame rows
            undifferentiated_cell_lines.append(
                DifferentiatedCellLine(
                    biomaterial_id=label,
                    description=row.get('undifferentiated_product.description'),
                    cell_line_biomaterial_id=parent_biomaterial_id,
                    differentiated_product_protocol_id=None,
                    undifferentiated_product_protocol_id=row.get(
                        'undifferentiated_product.undifferentiated_product_protocol_id'),
                    treatment_condition=row.get('undifferentiated_product.treatment_condition'),
                    wt_control_status=row.get('undifferentiated_product.wt_control_status'),
                    timepoint_value=row.get('undifferentiated_product.timepoint_value'),
                    timepoint_unit=row.get('undifferentiated_product.timepoint_unit'),
                    terminally_differentiated=row.get('undifferentiated_product.terminally_differentiated'),
                    model_system=row.get('undifferentiated_product.model_system'),
                    id=row.get('Id')
                )
            )

        return undifferentiated_cell_lines, df_filtered

    def parse_library_preparations(self,
                                   sheet_name,
                                   differentiated,
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
            'library_preparation.label',
            'differentiated_product.label',
            'undifferentiated_product.label',
            'library_preparation.library_preparation_protocol_id'
        ]

        for col in required_columns:
            if col not in df.columns:
                if col == 'differentiated_product.label' and differentiated:
                    errors.append(f"The column '{col}' does not exist in the {sheet_name} sheet. "
                                  f"The rest of the file will not be processed")

                    return [], df
                elif col == 'undifferentiated_product.label' and not differentiated:
                    errors.append(f"The column '{col}' does not exist in the {sheet_name} sheet. "
                                  f"The rest of the file will not be processed")

                    return [], df
                else:
                    if col not in ('differentiated_product.label', 'undifferentiated_product.label'):
                        errors.append(f"The column '{col}' does not exist in the {sheet_name} sheet. "
                                      f"The rest of the file will not be processed")

                        return [], df

        # Filter rows where biomaterial_id is not null
        df = df[df['library_preparation.label'].notna()]
        df = df.map(lambda x: None if isinstance(x, float) and (np.isnan(x) or not np.isfinite(x)) else x)
        # Define columns to check for values starting with 'ABC' or 'XYZ'
        cols_to_check = ['library_preparation.label']
        # Create a mask to filter rows where any of the specified columns start with 'ABC' or 'XYZ'
        mask = df[cols_to_check].apply(lambda x: ~x.astype(str).str.startswith(
            ('FILL OUT INFORMATION BELOW THIS ROW', 'A unique ID for the biomaterial.',
             'library_preparation.biomaterial_core.biomaterial_id'))).all(axis=1)
        # Apply the mask to filter out rows
        df_filtered = df[mask]
        # Check for mandatory fields and create Library Preparation objects
        library_preparations = []

        for _, row in df_filtered.iterrows():
            label = row['library_preparation.label']
            if differentiated:
                differentiated_biomaterial_label = row.get('differentiated_product.label')
            else:
                differentiated_biomaterial_label = row.get('undifferentiated_product.label')
            library_preparation_protocol_id = row.get('library_preparation.library_preparation_protocol_id')

            # Check if required fields are null
            if pd.isnull(label):
                errors.append("Library Preparation ID cannot be null in any row of the Library Preparation sheet.")
                # raise MissingMandatoryFieldError("Library Preparation ID cannot be null in any row.")
            if pd.isnull(differentiated_biomaterial_label):
                if differentiated:
                    errors.append(
                        "Differentiated Cell Line ID cannot be null in any row of the Library Preparation sheet.")
                    # raise MissingMandatoryFieldError("Differentiated Cell Line ID cannot be null in any row.")
                else:
                    errors.append(
                        "Undifferentiated Cell Line ID cannot be null in any row of the Library Preparation sheet.")
                    # raise MissingMandatoryFieldError("Differentiated Cell Line ID cannot be null in any row.")
            if pd.isnull(library_preparation_protocol_id):
                errors.append(
                    "Library Preparation Protocol ID cannot be null in any row of the Library Preparation sheet.")
                # raise MissingMandatoryFieldError("Library Preparation Protocol ID cannot be null in any row.")

            # Create LibraryPreparation objects from filtered DataFrame rows
            library_preparations.append(
                LibraryPreparation(
                    biomaterial_id=label,
                    protocol_id=library_preparation_protocol_id,
                    differentiated_biomaterial_id=differentiated_biomaterial_label,
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
            'sequence_file.label',
            'library_preparation.label',
            'sequence_file.extension',
            'sequence_file.read_index'
        ]

        for col in required_columns:
            if col not in df.columns:
                errors.append(f"The column '{col}' does not exist in the {sheet_name} sheet. "
                              f"The rest of the file will not be processed")

                return [], df

        # Filter rows where file_name is not null
        df = df[df['sequence_file.label'].notna()]
        df = df.map(lambda x: None if isinstance(x, float) and (np.isnan(x) or not np.isfinite(x)) else x)
        # Define columns to check for values starting with 'ABC' or 'XYZ'
        cols_to_check = ['sequence_file.label']
        # Create a mask to filter rows where any of the specified columns start with 'ABC' or 'XYZ'
        mask = df[cols_to_check].apply(lambda x: ~x.astype(str).str.startswith(
            ('FILL OUT INFORMATION BELOW THIS ROW', 'The name of the file.',
             'Include the file extension in the file name. For example: R1.fastq.gz; codebook.json',
             'sequence_file.label'))).all(axis=1)
        # Apply the mask to filter out rows
        df_filtered = df[mask]

        # Check for mandatory fields and create Sequencing file objects
        sequencing_files = []

        for _, row in df_filtered.iterrows():
            file_name = row['sequence_file.label']
            library_preparation_id = row.get('library_preparation.label')
            read_index = row.get('sequence_file.read_index')

            # Check if required fields are null
            if pd.isnull(file_name):
                errors.append("Sequence file name cannot be null in any row of the Sequencing File sheet.")
                # raise MissingMandatoryFieldError("Sequence file name cannot be null in any row.")
            if pd.isnull(library_preparation_id):
                errors.append("Library Preparation ID cannot be null in any row of the Sequencing File sheet..")
                # raise MissingMandatoryFieldError("Library Preparation ID cannot be null in any row.")
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
                    run_id=row.get('sequence_file.run_id'),
                    id=row.get('Id')
                )
            )

        return sequencing_files, df_filtered

    def parse_expression_alteration(self, sheet_name, action, errors):
        """
        Parses data related to expression alterations from a specified sheet in the Excel file.
        For datasets where the expression alteration tab is empty (e.g., UCSF), returns an empty list.
        """
        try:
            df = self.input_file_to_data_frames(sheet_name=sheet_name, action=action)
        except Exception as e:
            errors.append(f"Missing sheet '{sheet_name}': {e}")
            return [], None

        # If the DataFrame is empty or does not have the required column, return empty results.
        if df.empty or 'expression_alteration.label' not in df.columns:
            return [], df

        df.columns = df.columns.str.strip()

        required_columns = ['expression_alteration.label']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            errors.append(
                f"The following required columns are missing in the Expression Alteration Strategy sheet: {', '.join(missing_columns)}")
            return [], df

        # Filter rows where 'expression_alteration.label' is not null
        df = df[df['expression_alteration.label'].notna()]
        df = df.map(lambda x: None if isinstance(x, float) and (np.isnan(x) or not np.isfinite(x)) else x)

        unwanted_patterns = (
            'FILL OUT INFORMATION BELOW THIS ROW',
            'A unique ID for the gene expression alteration instance..',
            'ID should have no spaces. For example: JAXPE0001_MEIS1, MSKKI119_MEF2C, NWU_AID'
        )
        mask = df['expression_alteration.label'].astype(str).str.startswith(unwanted_patterns)
        df_filtered = df[~mask]

        expression_alterations = []
        for _, row in df_filtered.iterrows():
            expression_alterations.append(
                ExpressionAlterationStrategy(
                    expression_alteration_id=row.get('expression_alteration.label'),
                    parent_protocol_id=row.get('expression_alteration.parent_protocol_id'),
                    allele_specific=row.get('expression_alteration.genes.allele_specific'),
                    altered_gene_symbol=row.get('expression_alteration.genes.altered_gene_symbol'),
                    target_gene_hgnc_id=row.get('expression_alteration.genes.target_gene_hgnc_id'),
                    targeted_genomic_region=row.get('expression_alteration.genes.targeted_genomic_region'),
                    expected_alteration_type=row.get('expression_alteration.genes.expected_alteration_type'),
                    editing_strategy=row.get('expression_alteration.genes.editing_strategy'),
                    altered_locus=row.get('expression_alteration.genes.altered_locus'),
                    guide_sequence=row.get('expression_alteration.genes.guide_sequence'),
                    method=row.get('expression_alteration.method'),
                    id=row.get('Id')
                )
            )

        return expression_alterations, df_filtered

    def find_sheet_name(tab_names, candidates):
        """
        Find the first matching sheet name from a list of candidates.
        """
        for candidate in candidates:
            if candidate in tab_names:
                return candidate
        return None

    def parse_expression_alteration_with_genes(self, strategy_sheet, action, errors):
        """
        Parses pooled expression alteration strategy from the main tab and links all rows
        in the 'Expression alteration - Genes' tab to the single strategy that contains
        'various' in gene-related fields.

        Returns:
            Tuple[List[ExpressionAlterationStrategy], pd.DataFrame]
        """
        try:
            df = self.input_file_to_data_frames(sheet_name=strategy_sheet, action=action)
        except Exception as e:
            errors.append(f"Missing sheet '{strategy_sheet}': {e}")
            return [], None

        if df.empty or 'expression_alteration.label' not in df.columns:
            errors.append("Expression alteration sheet is empty or missing required column.")
            return [], df

        df.columns = df.columns.str.strip()
        df = df[df['expression_alteration.label'].notna()]
        df = df.applymap(lambda x: None if isinstance(x, float) and (np.isnan(x) or not np.isfinite(x)) else x)

        unwanted_patterns = (
            'FILL OUT INFORMATION BELOW THIS ROW',
            'A unique ID for the gene expression alteration instance..',
            'ID should have no spaces.'
        )
        mask = df['expression_alteration.label'].astype(str).str.startswith(unwanted_patterns)
        df_filtered = df[~mask]

        if df_filtered.empty:
            errors.append("No valid expression alteration strategy rows found.")
            return [], df_filtered

        # Expecting only one strategy (with 'various' gene info)
        strategy_row = df_filtered.iloc[0]
        label = strategy_row.get('expression_alteration.label')

        # Load gene tab
        try:
            available_tabs = self.list_sheets()
            gene_sheet_name = next(
                (name for name in ['expression_alteration_genes', 'Expression alteration - Genes'] if name in available_tabs),
                None
            )
            if not gene_sheet_name:
                raise ValueError("No gene-level sheet found for pooled expression alterations.")

            gene_df = self.input_file_to_data_frames(sheet_name=gene_sheet_name, action=action)
            gene_df.columns = gene_df.columns.str.strip()
            gene_df = gene_df[gene_df['expression_alteration.genes.altered_gene_symbol'].notna()]
            gene_df = gene_df.applymap(lambda x: None if isinstance(x, float) and (np.isnan(x) or not np.isfinite(x)) else x)

        except Exception as e:
            errors.append(f"Missing or unreadable gene-level sheet: {e}")
            return [], df_filtered

        # Convert gene rows to dicts
        genes = []
        flattened_records = []
        for _, gene_row in gene_df.iterrows():
            gene_data = {
                'allele_specific': gene_row.get('expression_alteration.genes.allele_specific'),
                'altered_gene_symbol': gene_row.get('expression_alteration.genes.altered_gene_symbol'),
                'target_gene_hgnc_id': gene_row.get('expression_alteration.genes.target_gene_hgnc_id'),
                'targeted_genomic_region': gene_row.get('expression_alteration.genes.targeted_genomic_region'),
                'expected_alteration_type': gene_row.get('expression_alteration.genes.expected_alteration_type'),
                'editing_strategy': gene_row.get('expression_alteration.genes.editing_strategy'),
                'altered_locus': gene_row.get('expression_alteration.genes.altered_locus'),
                'guide_sequence': gene_row.get('expression_alteration.genes.guide_sequence')
            }
            genes.append(gene_data)

            # Used later for writing into Excel
            flattened_records.append({
                'expression_alteration.label': label,
                'expression_alteration.parent_protocol_id': strategy_row.get('expression_alteration.parent_protocol_id'),
                'expression_alteration.method': strategy_row.get('expression_alteration.method'),
                'expression_alteration.genes.allele_specific': gene_data['allele_specific'],
                'expression_alteration.genes.altered_gene_symbol': gene_data['altered_gene_symbol'],
                'expression_alteration.genes.target_gene_hgnc_id': gene_data['target_gene_hgnc_id'],
                'expression_alteration.genes.targeted_genomic_region': gene_data['targeted_genomic_region'],
                'expression_alteration.genes.expected_alteration_type': gene_data['expected_alteration_type'],
                'expression_alteration.genes.editing_strategy': gene_data['editing_strategy'],
                'expression_alteration.genes.altered_locus': gene_data['altered_locus'],
                'expression_alteration.genes.guide_sequence': gene_data['guide_sequence'],
                'Id': strategy_row.get('Id')
            })

        if not genes:
            errors.append("No valid gene rows found in the gene tab.")
            return [], df_filtered

        # Construct strategy with gene list
        strategy = ExpressionAlterationStrategy(
            expression_alteration_id=label,
            parent_protocol_id=strategy_row.get('expression_alteration.parent_protocol_id'),
            method=strategy_row.get('expression_alteration.method'),
            id=strategy_row.get('Id'),
            genes=genes
        )

        expression_alterations_df = pd.DataFrame(flattened_records)
        print("Parsed expression alterations:", len(flattened_records))
        return [strategy], expression_alterations_df

    def get_cell_lines(self,
                       sheet_name,
                       action,
                       errors,
                       context=None):
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
        cell_lines, cell_lines_df, parent_cell_line_names = self.parse_cell_lines(sheet_name, action, errors, context)
        return cell_lines, cell_lines_df, parent_cell_line_names

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
                                 differentiated,
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
        library_preparations, df_filtered = self.parse_library_preparations(sheet_name, differentiated,
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
                                   errors,
                                   context=None):
        """
        Retrieves parsed expression alterations from the appropriate sheet(s) in the Excel file.

        Parameters:
            sheet_name (str): Name of the main expression alteration sheet.
            action (str): Submission action (ADD, MODIFY, DELETE).
            errors (list): A list to collect validation or parsing errors.
            context (str, optional): Ingestion context to distinguish between formats.
                                     e.g., 'pooled_differentiated' for MSK-style pooled datasets.

        Returns:
            Tuple[List[ExpressionAlterationStrategy], DataFrame]: Parsed strategies and cleaned DataFrame.
        """
        if context == 'pooled_differentiated':
            print("Using pooled_differentiated parsing: augmenting expression alterations with gene-specific info "
                  "from 'expression_alteration_genes' tab.")
            return self.parse_expression_alteration_with_genes(sheet_name, action, errors)
        else:
            return self.parse_expression_alteration(sheet_name, action, errors)
