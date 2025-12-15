import os
import sys
import logging
from datetime import datetime

import numpy as np
import pandas as pd

from ait.commons.util.storage.factory import build_storage
from ait.commons.util.command.list import CmdList
from ait.commons.util.command.submit import (
    CmdSubmit,
    get_entity_id_from_hal_link,
    create_new_submission_envelope,
)
from ait.commons.util.command.upload import CmdUpload
from ait.commons.util.user_profile import get_profile
from ait.commons.util.provider_api_util import ProviderApi
from ait.commons.util.spreadsheet_util import (
    SpreadsheetSubmitter,
    ValidationError,
    merge_library_preparation_sequencing_file,
    merge_cell_line_and_differentiated_cell_line,
    merge_differentiated_cell_line_and_library_preparation,
    SubmissionError,
    process_library_preparations,
)

from ait.commons.util.settings.morphic_util import BASE_URL
from ait.commons.util.storage.globus_backend import _load_globus_config, _get_ingest_bearer_token

log = logging.getLogger("morphic-util")

try:
    _GLOBUS_CFG = _load_globus_config()
except Exception:
    _GLOBUS_CFG = {}


# -----------------------------
# Logging setup
# -----------------------------
def setup_logging(args):
    """
    Call once early (ideally in CLI entrypoint).
    If you can't, calling in CmdSubmitFile.__init__ is okay as a fallback.
    """
    level = logging.WARNING
    if getattr(args, "debug", False):
        level = logging.DEBUG
    elif getattr(args, "verbose", False):
        level = logging.INFO

    # basicConfig is a no-op if logging already configured elsewhere.
    logging.basicConfig(level=level, format="%(levelname)s: %(message)s")


# -----------------------------
# Validation report formatting
# -----------------------------
def format_validation_report(*, dataset, spreadsheet, parsed_summary=None, errors=None, warnings=None):
    errors = errors or []
    warnings = warnings or []

    lines = []
    lines.append(
        f"\n❌ Validation failed ({len(errors)} error{'s' if len(errors)!=1 else ''}, "
        f"{len(warnings)} warning{'s' if len(warnings)!=1 else ''}) — dataset {dataset}"
    )

    if spreadsheet:
        lines.append(f"   Spreadsheet: {os.path.basename(spreadsheet)}")

    if parsed_summary:
        lines.append("")
        lines.append("Parsed summary")
        for k, v in parsed_summary.items():
            lines.append(f"  - {k}: {v}")

    if errors:
        lines.append("")
        lines.append("ERRORS (must fix)")
        for e in errors:
            lines.append("  " + str(e).replace("\n", "\n  "))

    if warnings:
        lines.append("")
        lines.append("WARNINGS (check)")
        for w in warnings:
            lines.append("  " + str(w).replace("\n", "\n  "))

    lines.append("")
    lines.append("Next steps")
    lines.append("  1) Upload missing files or correct filenames in the spreadsheet.")
    lines.append("  2) Remove/ignore extra files, or reference them in the spreadsheet if intended.")
    return "\n".join(lines)


def _normalize_list_entry(entry) -> str | None:
    """
    CmdList output can be:
      - strings
      - tuples/lists
    Normalize to a "path-like first field" string.
    """
    if entry is None:
        return None

    if isinstance(entry, (tuple, list)) and entry:
        candidate = str(entry[0]).strip()
    else:
        raw = str(entry).strip()
        if not raw:
            return None
        candidate = raw.split()[0].strip()

    if not candidate:
        return None
    return candidate


def validate_sequencing_files(
    sequencing_files,
    list_of_files_in_upload_area,
    dataset,
    spreadsheet_filename=None,
):
    """
    Returns:
      (errors, warnings) where each is a list[str].
    """
    sheet_files = {
        (getattr(sf, "file_name", "") or "").strip()
        for sf in (sequencing_files or [])
        if (getattr(sf, "file_name", "") or "").strip()
    }

    storage_basenames = set()
    for entry in (list_of_files_in_upload_area or []):
        candidate = _normalize_list_entry(entry)
        if not candidate:
            continue

        # skip table headers / separators (in case CmdList returns such lines)
        if candidate.startswith("Name") or candidate.startswith("---"):
            continue

        # only consider "file-like" entries
        if "." not in candidate:
            continue

        basename = os.path.basename(candidate)

        # ignore generated / non-data artifacts
        if spreadsheet_filename and basename == spreadsheet_filename:
            continue
        if basename.startswith("submission_result_") and basename.lower().endswith(".xlsx"):
            continue

        storage_basenames.add(basename)

    missing = sorted(sheet_files - storage_basenames)
    extra = sorted(storage_basenames - sheet_files)

    errors: list[str] = []
    warnings: list[str] = []

    if missing:
        errors.append(
            f"Missing sequencing files in upload area ({len(missing)}):\n"
            + "\n".join([f"  - {m}" for m in missing])
        )

    if extra:
        warnings.append(
            f"Extra files in upload area not referenced by spreadsheet ({len(extra)}):\n"
            + "\n".join([f"  - {x}" for x in extra])
        )

    return errors, warnings


def get_content(unique_value):
    return {"content": unique_value}


def _create_expression_alterations(
    submission_instance,
    submission_envelope_id,
    access_token,
    expression_alterations,
    expression_alterations_df,
):
    expression_alterations_entity_id_column_name = "Id"

    if expression_alterations_entity_id_column_name not in expression_alterations_df.columns:
        expression_alterations_df[expression_alterations_entity_id_column_name] = np.nan

    for expression_alteration in expression_alterations:
        expression_alteration_id = submission_instance.use_existing_envelope_and_submit_entity(
            "process",
            expression_alteration.to_dict(),
            submission_envelope_id,
            access_token,
        )
        expression_alteration.id = expression_alteration_id

        expression_alterations_df[expression_alterations_entity_id_column_name] = (
            expression_alterations_df[expression_alterations_entity_id_column_name].astype(object)
        )

        expression_alterations_df.loc[
            expression_alterations_df["expression_alteration.label"] == expression_alteration.expression_alteration_id,
            expression_alterations_entity_id_column_name,
        ] = expression_alteration_id

    return expression_alterations


class CmdSubmitFile:
    BASE_URL = BASE_URL
    SUBMISSION_ENVELOPE_CREATE_URL = f"{BASE_URL}/submissionEnvelopes/updateSubmissions"
    SUBMISSION_ENVELOPE_BASE_URL = f"{BASE_URL}/submissionEnvelopes"

    def __init__(self, args):
        self.args = args

        # If CLI entrypoint didn't call setup_logging, do it here (safe-ish fallback)
        setup_logging(args)

        self.user_profile = get_profile("morphic-util")

        # Prefer Globus access token for Provider API calls
        try:
            self.access_token = _get_ingest_bearer_token(_GLOBUS_CFG)
            log.info("Using Globus access token for Provider API calls")
        except Exception:
            log.info("Globus auth not available; using profile access token")
            self.access_token = self.user_profile.access_token

        self.storage = build_storage(self.user_profile)
        self.provider_api = ProviderApi(self.BASE_URL)

        self.validation_errors: list[str] = []
        self.validation_warnings: list[str] = []

        self.submission_errors: list[str] = []
        self.submission_envelope_id = None

        self.context = getattr(args, "context", None)

        self.action = self._get_required_arg("action", "Submission action (ADD, MODIFY or DELETE) is mandatory")
        self.dataset = self._get_required_arg(
            "dataset",
            (
                "Dataset is mandatory to be registered before submitting dataset metadata. "
                "Please submit your study using the submit option, register your dataset using "
                "the submit option, and link your dataset to your study before proceeding with this submission."
            ),
        )

        if self.dataset:
            try:
                self.provider_api.get(f"{self.BASE_URL}/datasets/{self.dataset}", self.access_token)
            except Exception:
                print(f"Dataset does not exist {self.dataset}")
                sys.exit(1)

        if self.action != "DELETE":
            self.file = self._get_required_arg("file", "File is mandatory")
        else:
            self.file = None
            print(f"Deleting dataset {self.dataset}")

    def _get_required_arg(self, attr_name, error_message):
        value = getattr(self.args, attr_name, None)
        if value is None:
            print(error_message)
            sys.exit(1)
        return value

    def run(self):
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
            print("\nProcess interrupted by user. Exiting gracefully...")
            self._delete_actions(self.submission_envelope_id, submission_instance, None)
            sys.exit(0)
        except Exception as e:
            print(f"An unexpected error occurred: {str(e)}")
            self._delete_actions(self.submission_envelope_id, submission_instance, None)
            sys.exit(1)

    def _is_delete_action(self):
        return str(self.action).lower() == "delete"

    def _handle_delete(self, submission_instance):
        submission_instance.delete_dataset(self.dataset, self.access_token)
        return True, None

    def _list_files_in_upload_area(self):
        list_instance = CmdList(self.storage, self.args)
        return list_instance.list_bucket_contents_and_return(self.dataset, "")

    def _process_submission(self, submission_instance, list_of_files_in_upload_area):
        try:
            """Process the file submission."""
            parser = SpreadsheetSubmitter(self.file)
            parsed_data = self._parse_spreadsheet(parser)
            self._validate_and_upload(parsed_data, list_of_files_in_upload_area)

            # Extract parsed data
            expression_alterations = parsed_data['expression_alterations']
            expression_alterations_df = parsed_data['expression_alterations_df']
            parent_cell_line_names = parsed_data['parent_cell_line_names']
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

            if cell_lines and cell_lines_df is not None:
                if self._is_add_action():
                    created_expression_alterations = self._handle_expression_alterations(
                        submission_instance,
                        expression_alterations,
                        expression_alterations_df,
                        parent_cell_line_names,
                        cell_lines
                    )

                created_cell_lines = self._create_cell_lines(
                    submission_instance, cell_lines, cell_lines_df, created_expression_alterations)

            if differentiated_cell_lines and differentiated_cell_lines_df is not None:
                created_differentiated_or_undifferentiated_cell_lines = self._create_differentiated_cell_lines(
                    submission_instance, differentiated_cell_lines, differentiated_cell_lines_df, differentiated)

            if (undifferentiated_cell_lines and undifferentiated_cell_lines_df is not None
                    and not differentiated):
                created_differentiated_or_undifferentiated_cell_lines = self._create_differentiated_cell_lines(
                    submission_instance, undifferentiated_cell_lines, undifferentiated_cell_lines_df, differentiated)

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
            # If we raise ValidationError([report]), print it cleanly:
            for msg in getattr(e, "errors", []) or ["Validation Error"]:
                print(msg)
            sys.exit(1)

        except SubmissionError as e:
            print(f"Submission Error: {e.errors}")
            self._delete_actions(self.submission_envelope_id, submission_instance, e)
            sys.exit(1)

        except Exception as e:
            print(f"An unexpected error occurred during submission processing: {e}")
            self._delete_actions(self.submission_envelope_id, submission_instance, e)
            raise

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
                                       parent_cell_line_names,
                                       cell_lines):
        """Handles the creation of expression alterations and links them to the parent cell line if needed."""
        created_expression_alterations = []

        if expression_alterations and expression_alterations_df is not None:
            created_expression_alterations = self._submit_expression_alterations(
                submission_instance, expression_alterations, expression_alterations_df
            )

        if created_expression_alterations:
            for parent_cell_line_name in parent_cell_line_names:
                self._link_parent_cell_line_expression_alteration(
                    submission_instance,
                    self.access_token,
                    parent_cell_line_name,
                    cell_lines,
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
                'Expression alteration', self.action, self.validation_errors,
                context=self.context
            )

            cell_lines, cell_lines_df, parent_cell_line_names = parser.get_cell_lines(
                cell_line_sheet_name, self.action, self.validation_errors, context=self.context
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
                                                             self.validation_errors, context=self.context)

            if undifferentiated_cell_lines and not differentiated:
                merge_cell_line_and_differentiated_cell_line(cell_lines, undifferentiated_cell_lines,
                                                             self.validation_errors, context=self.context)

            library_preparations_result = parser.get_library_preparations(
                'Library preparation', differentiated, self.action, self.validation_errors)

            if not isinstance(library_preparations_result, tuple) or len(library_preparations_result) != 2:
                raise ValueError("Unexpected return from get_library_preparations()")

            library_preparations, library_preparations_df = library_preparations_result

            # Handle N:1 relationships for differentiated products in library preparation
            for lp in library_preparations:
                if "differentiated_biomaterial_id" in lp.__dict__:
                    differentiated_ids = lp.differentiated_biomaterial_id.split("|")
                    lp.differentiated_biomaterial_id = differentiated_ids

            if differentiated_cell_lines:
                if self.context == "unperturbed_multiple":
                    # Use the new processing that creates a LP process and links the clone and differentiated product
                    process_library_preparations(cell_lines, differentiated_cell_lines, library_preparations, self.validation_errors)
                else:
                    # Use the original merge function for differentiated cell lines (for MSK, JAX, etc.)
                    merge_differentiated_cell_line_and_library_preparation(differentiated_cell_lines,
                                                                           library_preparations, self.validation_errors, cell_lines=cell_lines)
            elif undifferentiated_cell_lines and not differentiated:
                if self.context == "unperturbed_multiple":
                    process_library_preparations(cell_lines, undifferentiated_cell_lines, library_preparations, self.validation_errors)
                else:
                    merge_differentiated_cell_line_and_library_preparation(undifferentiated_cell_lines,
                                                                           library_preparations, self.validation_errors, cell_lines=cell_lines)

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
                "parent_cell_line_names": parent_cell_line_names,
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
        except Exception as e:
            print(f"Exception occurred:", e)

            self.validation_errors.append(f"Spreadsheet is invalid {self.file}")
            return None

    def _validate_and_upload(self, parsed_data, list_of_files_in_upload_area):
        sequencing_files = (parsed_data or {}).get("sequencing_files") or []
        spreadsheet_basename = os.path.basename(self.file) if self.file else None

        file_errors, file_warnings = validate_sequencing_files(
            sequencing_files=sequencing_files,
            list_of_files_in_upload_area=list_of_files_in_upload_area,
            dataset=self.dataset,
            spreadsheet_filename=spreadsheet_basename,
        )

        # Combine any spreadsheet-structural errors already gathered + file presence errors
        all_errors = list(self.validation_errors) + list(file_errors)
        self.validation_warnings = list(file_warnings)

        if all_errors:
            report = format_validation_report(
                dataset=self.dataset,
                spreadsheet=self.file,
                parsed_summary={
                    "cell lines": len((parsed_data or {}).get("cell_lines") or []),
                    "library preparations": len((parsed_data or {}).get("library_preparations") or []),
                    "sequencing files": len(sequencing_files),
                } if parsed_data else None,
                errors=all_errors,
                warnings=self.validation_warnings,
            )
            raise ValidationError([report])

        # Validation succeeded: optionally show warnings (or only show in --verbose)
        if self.validation_warnings:
            # If you want these only in verbose mode, swap print -> log.info
            for w in self.validation_warnings:
                print(f"⚠ WARNING: {w}\n")

        print(f"✅ Spreadsheet + upload area validated — dataset {self.dataset}")
        print(f"Uploading {spreadsheet_basename} to storage...")

        CmdUpload(self.storage, self.args).upload_file(
            self.dataset,
            self.file,
            spreadsheet_basename,
            1,
            1,
        )

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
                                          differentiated_cell_lines_df,
                                          differentiated):
        for differentiated_cell_line in differentiated_cell_lines:
            differentiated_cell_line_entity_id = submission_instance.handle_differentiated_cell_line(None,
                                                                                                     differentiated_cell_line,
                                                                                                     differentiated_cell_lines_df,
                                                                                                     differentiated,
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
            self.submission_errors,
            context=self.context
        )

        return updated_dfs, message

    def _save_and_upload_results(self,
                                 updated_dfs,
                                 expression_alteration_df,
                                 cell_line_sheet_name,
                                 differentiated_or_undifferentiated_cell_line_sheet_name):
        """Save the updated dataframes and upload the results.

        The submission result file is written next to the original spreadsheet
        (self.file) so that it lives under the same Globus-local root.
        """
        current_time = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        output_basename = f"submission_result_{current_time}.xlsx"

        # Write the workbook into the same directory as the input spreadsheet
        # so Globus can see it on the local endpoint.
        input_dir = os.path.dirname(os.path.abspath(self.file))
        if not input_dir:
            input_dir = os.getcwd()

        output_file = os.path.join(input_dir, output_basename)

        # -----------------------------
        # 1) Build the Excel workbook
        # -----------------------------
        try:
            # Expand gene info if in pooled mode
            if self.context == 'pooled_differentiated':
                print("Expanding expression alteration strategies for pooled_differentiated mode...")
                if expression_alteration_df is not None and not expression_alteration_df.empty:
                    # Check if it's already flat
                    if "expression_alteration.genes.altered_gene_symbol" in expression_alteration_df.columns:
                        print("expression_alteration_df already flattened — skipping expansion.")
                    else:
                        expanded_rows = []
                        for _, row in expression_alteration_df.iterrows():
                            genes = row.get("genes", [])
                            if isinstance(genes, list):
                                for gene in genes:
                                    expanded_rows.append({
                                        'expression_alteration.label': row.get('expression_alteration.label'),
                                        'expression_alteration.parent_protocol_id': row.get('expression_alteration.parent_protocol_id'),
                                        'expression_alteration.method': row.get('expression_alteration.method'),
                                        'expression_alteration.genes.allele_specific': gene.get('allele_specific'),
                                        'expression_alteration.genes.altered_gene_symbol': gene.get('altered_gene_symbol'),
                                        'expression_alteration.genes.target_gene_hgnc_id': gene.get('target_gene_hgnc_id'),
                                        'expression_alteration.genes.targeted_genomic_region': gene.get('targeted_genomic_region'),
                                        'expression_alteration.genes.expected_alteration_type': gene.get('expected_alteration_type'),
                                        'expression_alteration.genes.editing_strategy': gene.get('editing_strategy'),
                                        'expression_alteration.genes.altered_locus': gene.get('altered_locus'),
                                        'expression_alteration.genes.guide_sequence': gene.get('guide_sequence'),
                                        'Id': row.get('Id')
                                    })
                            else:
                                print(f"Skipping row without gene list: {row}")
                        expression_alteration_df = pd.DataFrame(expanded_rows)
                else:
                    print("expression_alteration_df is empty or None — no gene info expanded.")

            print(f"Preparing submission result file: {output_file}")

            dataframes = [
                (updated_dfs[0], cell_line_sheet_name),
                (updated_dfs[1], differentiated_or_undifferentiated_cell_line_sheet_name),
                (updated_dfs[2], 'Library preparation'),
                (updated_dfs[3], 'Sequence file'),
                (expression_alteration_df, 'Expression alteration strategy'),
            ]

            # Actually write the Excel file
            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                for df, sheet_name in dataframes:
                    if df is None:
                        print(f"Skipping sheet '{sheet_name}' — DataFrame is None")
                        continue
                    if df.empty:
                        print(f"Skipping sheet '{sheet_name}' — DataFrame is empty")
                        continue
                    print(f"Writing sheet '{sheet_name}' with shape {df.shape}")
                    df.to_excel(writer, sheet_name=sheet_name, index=False)

        except Exception as e:
            print(f"Failed to generate submission result workbook {output_file}. Error: {e}")
            print(f"Refer to dataset '{self.dataset}' for metadata tracing.")
            raise

        # ---------------------------------
        # 2) Upload the workbook via Globus
        # ---------------------------------
        if not os.path.exists(output_file):
            raise FileNotFoundError(
                f"The output file {output_file} was not created or cannot be found."
            )

        try:
            uploader = CmdUpload(self.storage, self.args)
            uploader.upload_file(
                self.dataset,
                output_file,      # full path on the local endpoint
                output_basename,  # name in the destination area
                1,
                1,
            )
            print(f"File {output_basename} uploaded successfully from {output_file}.")
        except KeyboardInterrupt:
            print(
                f"\nUpload of {output_basename} was interrupted by the user. "
                "The submission result file may not have been fully transferred."
            )
            raise
        except Exception as e:
            print(f"Failed to upload file {output_basename}. Error: {e}")
            print(f"Refer to dataset '{self.dataset}' for metadata tracing.")
            raise

    def _delete_actions(self, submission_envelope_id, submission_instance, error=None):
        """Handle actions needed when a submission fails."""
        try:
            if self._is_add_action():
                return self._handle_add_action_failure(
                    submission_envelope_id,
                    submission_instance,
                    error
                )
            elif self._is_modify_action():
                return self._handle_modify_action_failure(error)
            else:
                # Fallback if some unexpected action type appears
                return False, "Submission failed; unknown action type during rollback."
        except Exception as e:
            print(f"Failed to rollback submission {submission_envelope_id}: {str(e)}")
            return False, f"Failed to rollback submission {submission_envelope_id}: {str(e)}"

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
                                                     cell_lines,
                                                     created_expression_alterations):
        parent_cell_line_id = self._handle_parent_cell_line(submission_instance, parent_cell_line_name)

        for cell_line in cell_lines:
            if cell_line.parental_cell_line_name == parent_cell_line_name:
                for expression_alteration in created_expression_alterations:
                    if cell_line.expression_alteration_id == expression_alteration.expression_alteration_id:
                        print(f"Expression alteration match found, Linking parent cell line {parent_cell_line_name} "
                              f"as input to process of {expression_alteration.expression_alteration_id}")
                        submission_instance.perform_hal_linkage(
                            f"{self.BASE_URL}/biomaterials/{parent_cell_line_id}/inputToProcesses",
                            expression_alteration.id, 'processes', access_token
                        )
