"""
Standalone spreadsheet validation/parsing logic shared by:

  * CmdSubmitFile (the CLI submit-file command)
  * The HTTP /validate service (service/app.py)

The CLI passes its own action/context/validation_errors so existing behavior
is preserved (validation_errors is mutated in place). The HTTP service can
call this with action=None, context=None, validation_errors=None and read
the errors back from the returned dict.
"""

from .spreadsheet_util import (
    merge_cell_line_and_differentiated_cell_line,
    merge_differentiated_cell_line_and_library_preparation,
    merge_library_preparation_sequencing_file,
    process_library_preparations,
    ValidationError,
)


class SpreadsheetValidator:
    @staticmethod
    def _parse_spreadsheet(parser, action=None, context=None, validation_errors=None):
        """
        Parse a spreadsheet (already opened via SpreadsheetSubmitter) into a
        structured dict, accumulating non-fatal validation errors in
        ``validation_errors`` (mutated in place if provided).

        Parameters
        ----------
        parser : SpreadsheetSubmitter
            The opened spreadsheet.
        action : str | None
            ADD / MODIFY / DELETE for the CLI; None for the HTTP validator.
        context : str | None
            Optional ingestion context (e.g. ``pooled_differentiated``,
            ``unperturbed_multiple``).
        validation_errors : list | None
            Existing errors list to append to. If None, a new list is created.

        Returns
        -------
        dict
            Parsed sections plus ``errors`` and ``sheets`` keys for callers
            (the HTTP service) that don't have a separate handle on the
            mutated ``validation_errors`` list.
        """
        if validation_errors is None:
            validation_errors = []

        try:
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
                validation_errors.append(
                    "Spreadsheet must contain a 'Cell line' or 'Clonal cell line' sheet."
                )

            if not (differentiated_cell_line_sheet_name or undifferentiated_cell_line_sheet_name):
                validation_errors.append(
                    "Spreadsheet must contain a "
                    "'Differentiated cell line', 'Undifferentiated product', "
                    "or 'Differentiated product' sheet."
                )

            # Parse different sections of the spreadsheet
            expression_alterations, expression_alterations_df = parser.get_expression_alterations(
                'Expression alteration', action, validation_errors,
                context=context
            )

            cell_lines, cell_lines_df, parent_cell_line_names = parser.get_cell_lines(
                cell_line_sheet_name, action, validation_errors, context=context
            )

            if differentiated_cell_line_sheet_name:
                differentiated_cell_lines, differentiated_cell_lines_df = parser.get_differentiated_cell_lines(
                    differentiated_cell_line_sheet_name, action, validation_errors
                )

            if undifferentiated_cell_line_sheet_name:
                undifferentiated_cell_lines, undifferentiated_cell_lines_df = parser.get_undifferentiated_cell_lines(
                    undifferentiated_cell_line_sheet_name, action, validation_errors
                )

            # Check for errors and merge data
            if differentiated_cell_lines and undifferentiated_cell_lines:
                validation_errors.append(
                    "A spreadsheet cannot contain rows in both differentiated and undifferentiated cell lines/ products"
                )

            if differentiated_cell_lines:
                differentiated = True
                merge_cell_line_and_differentiated_cell_line(
                    cell_lines, differentiated_cell_lines, validation_errors, context=context
                )

            if undifferentiated_cell_lines and not differentiated:
                merge_cell_line_and_differentiated_cell_line(
                    cell_lines, undifferentiated_cell_lines, validation_errors, context=context
                )

            library_preparations_result = parser.get_library_preparations(
                'Library preparation', differentiated, action, validation_errors
            )

            if not isinstance(library_preparations_result, tuple) or len(library_preparations_result) != 2:
                raise ValueError("Unexpected return from get_library_preparations()")

            library_preparations, library_preparations_df = library_preparations_result

            # Handle N:1 relationships for differentiated products in library preparation
            for lp in library_preparations:
                if "differentiated_biomaterial_id" in lp.__dict__:
                    differentiated_ids = lp.differentiated_biomaterial_id.split("|")
                    lp.differentiated_biomaterial_id = differentiated_ids

            if differentiated_cell_lines:
                if context == "unperturbed_multiple":
                    process_library_preparations(
                        cell_lines, differentiated_cell_lines, library_preparations, validation_errors
                    )
                else:
                    merge_differentiated_cell_line_and_library_preparation(
                        differentiated_cell_lines, library_preparations, validation_errors,
                        cell_lines=cell_lines
                    )
            elif undifferentiated_cell_lines and not differentiated:
                if context == "unperturbed_multiple":
                    process_library_preparations(
                        cell_lines, undifferentiated_cell_lines, library_preparations, validation_errors
                    )
                else:
                    merge_differentiated_cell_line_and_library_preparation(
                        undifferentiated_cell_lines, library_preparations, validation_errors,
                        cell_lines=cell_lines
                    )

            sequencing_files, sequencing_files_df = parser.get_sequencing_files(
                'Sequence file', action, validation_errors
            )

            merge_library_preparation_sequencing_file(
                library_preparations, sequencing_files, validation_errors
            )

            # Return parsed data as a dictionary. The CLI ignores the extra
            # 'errors' / 'sheets' keys (it reads self.validation_errors); the
            # HTTP service reads them directly from this dict.
            return {
                "sheets": tab_names,
                "errors": validation_errors,
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
                "undifferentiated_cell_line_sheet_name": undifferentiated_cell_line_sheet_name,
            }
        except Exception:
            # Re-raise; caller decides how to surface (CLI prints, HTTP returns 400/500)
            raise
