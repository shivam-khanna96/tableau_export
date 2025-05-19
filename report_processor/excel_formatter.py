# tableau_admissions_report/report_processor/excel_formatter.py
import logging
from openpyxl import load_workbook
from openpyxl.worksheet.worksheet import Worksheet
from openpyxl.utils import get_column_letter #, range_boundaries # range_boundaries not used
from openpyxl.styles import Alignment, Font, Border, Side, PatternFill
from typing import List

from config import settings

logger = logging.getLogger(__name__)

def _auto_adjust_column_widths(ws: Worksheet, padding: int = 5, max_width: int = 100):
    """Adjusts column widths based on the maximum content length in each column."""
    logger.debug(f"Adjusting column widths for sheet: {ws.title}")
    merged_cells_in_sheet = set()
    for merged_range_obj in ws.merged_cells.ranges:
        for row, col in merged_range_obj.cells: # type: ignore
            merged_cells_in_sheet.add(ws.cell(row=row, column=col).coordinate)

    for col_cells in ws.columns:
        max_length = 0
        column_letter = get_column_letter(col_cells[0].column)
        for cell in col_cells:
            if cell.coordinate in merged_cells_in_sheet:
                continue
            try:
                if cell.value:
                    cell_value_str = str(cell.value)
                    if len(cell_value_str) > max_length:
                        max_length = len(cell_value_str)
            except:
                pass
        adjusted_width = (max_length + padding) if max_length > 0 else 15
        ws.column_dimensions[column_letter].width = min(adjusted_width, max_width)
    logger.debug(f"Column widths adjusted for sheet: {ws.title}")


# <<< MODIFIED FUNCTION to be more generic >>>
def _apply_detailed_report_styles(ws: Worksheet,
                                  final_column_order_list: List[str],
                                  sheet_title_for_logging: str):
    """
    Applies specific formatting (like Progress Report style) to a sheet.
    Includes merging cells, applying fonts, fills, borders, and alignment.
    Parameterized to accept sheet-specific configurations.
    """
    logger.info(f"Applying detailed styles to '{sheet_title_for_logging}' sheet: {ws.title}")

    ws.freeze_panes = 'A2'
    logger.debug(f"Froze top row for sheet: {ws.title}")

    if ws.max_row <= 1:
        logger.warning(f"Sheet '{ws.title}' is empty or has only headers. Applying basic styling.")
        if ws.max_row == 1:
             for cell in ws[1]:
                cell.font = Font(bold=settings.EXCEL_FONT_BOLD)
                cell.alignment = Alignment(**settings.EXCEL_ALIGNMENT_CENTER)
        _auto_adjust_column_widths(ws)
        return

    font_bold = Font(bold=settings.EXCEL_FONT_BOLD)
    align_center_center = Alignment(**settings.EXCEL_ALIGNMENT_CENTER)
    fill_alt_row = PatternFill(**settings.EXCEL_FILL_ALT_ROW)
    fill_term_block_alt = PatternFill(**settings.EXCEL_FILL_ALT_ROW) # Same as alt_row for term block
    fill_total_row = PatternFill(**settings.EXCEL_FILL_TOTAL_ROW)
    fill_grand_total_row = PatternFill(**settings.EXCEL_FILL_GRAND_TOTAL_ROW)
    side_thin_white = Side(**settings.EXCEL_BORDER_SIDE_THIN_WHITE)
    side_medium_black = Side(**settings.EXCEL_BORDER_SIDE_MEDIUM_BLACK)
    # border_thin_white_all_sides = Border(left=side_thin_white, right=side_thin_white, top=side_thin_white, bottom=side_thin_white) # Not used

    try:
        # Use the provided final_column_order_list to find indices
        term_col_idx = final_column_order_list.index("Application Term") + 1
        program_col_idx = final_column_order_list.index("Program") + 1
        degree_col_idx = final_column_order_list.index("DEGREE") + 1
        first_data_col_idx = degree_col_idx + 1
    except ValueError as e:
        logger.error(f"Key styling column (Application Term, Program, or DEGREE) not in final_column_order_list for sheet '{ws.title}': {e}. Styling may be incorrect.")
        _auto_adjust_column_widths(ws)
        return

    # --- 1. Merge 'Application Term' cells ---
    logger.debug(f"Merging 'Application Term' cells for '{ws.title}'...")
    current_term_value_being_grouped = None
    merge_start_row_for_current_term = 2

    for row_idx_iterator in range(2, ws.max_row + 2):
        value_in_current_cell = ws.cell(row=row_idx_iterator, column=term_col_idx).value if row_idx_iterator <= ws.max_row else None
        is_current_cell_grand_total_label = (isinstance(value_in_current_cell, str) and value_in_current_cell == "Grand Total")


        if (value_in_current_cell != current_term_value_being_grouped or row_idx_iterator == ws.max_row + 1) and \
           current_term_value_being_grouped is not None:
            if current_term_value_being_grouped != "Grand Total":
                end_row_for_this_merge = (row_idx_iterator - 1) if row_idx_iterator <= ws.max_row else ws.max_row
                if end_row_for_this_merge >= merge_start_row_for_current_term:
                    try:
                        # Check if start and end rows are the same. If so, no need to merge a single cell.
                        if merge_start_row_for_current_term < end_row_for_this_merge :
                            ws.merge_cells(start_row=merge_start_row_for_current_term,
                                           end_row=end_row_for_this_merge,
                                           start_column=term_col_idx,
                                           end_column=term_col_idx)
                            logger.debug(f"Merged 'Application Term' for '{current_term_value_being_grouped}' from row {merge_start_row_for_current_term} to {end_row_for_this_merge} on sheet '{ws.title}'")
                        # Always apply alignment, even if not merged (single row group)
                        ws.cell(row=merge_start_row_for_current_term, column=term_col_idx).alignment = align_center_center

                    except Exception as e_merge:
                        logger.warning(f"Could not merge 'Application Term' cells for '{current_term_value_being_grouped}' on sheet '{ws.title}': {e_merge}")

        if is_current_cell_grand_total_label:
            logger.debug(f"Encountered 'Grand Total' label at row {row_idx_iterator} on sheet '{ws.title}'. Stopping 'Application Term' group merging.")
            break
        if row_idx_iterator > ws.max_row:
            break
        if value_in_current_cell != current_term_value_being_grouped:
            current_term_value_being_grouped = value_in_current_cell
            merge_start_row_for_current_term = row_idx_iterator
    logger.debug(f"'Application Term' cell merging complete for '{ws.title}'.")

    # --- Apply Alternating Fill to Merged "Application Term" Blocks ---
    # (This logic should still work fine for single or multiple term blocks)
    logger.debug(f"Applying alternating fill to 'Application Term' merged blocks for '{ws.title}'...")
    term_merged_ranges = []
    for merged_range_obj in ws.merged_cells.ranges:
        min_col, _, max_col, _ = merged_range_obj.bounds # type: ignore
        if min_col == term_col_idx and max_col == term_col_idx:
            term_merged_ranges.append(merged_range_obj)
    term_merged_ranges.sort(key=lambda r: r.min_row) # type: ignore

    apply_alternate_fill_to_term_block = True
    for term_range in term_merged_ranges:
        top_left_cell_value = ws.cell(row=term_range.min_row, column=term_range.min_col).value # type: ignore
        if isinstance(top_left_cell_value, str) and top_left_cell_value.strip() == "Grand Total":
            continue
        if apply_alternate_fill_to_term_block:
            for row_idx_in_merge in range(term_range.min_row, term_range.max_row + 1): # type: ignore
                ws.cell(row=row_idx_in_merge, column=term_col_idx).fill = fill_term_block_alt
        apply_alternate_fill_to_term_block = not apply_alternate_fill_to_term_block
    logger.debug(f"Alternating fill for 'Application Term' blocks applied for '{ws.title}'.")


    # --- 2. Merge 'Total' (Program) and 'Grand Total' (Term) label cells ---
    logger.debug(f"Merging 'Total' and 'Grand Total' label cells for '{ws.title}'...")
    for row_idx in range(2, ws.max_row + 1):
        program_cell_value = str(ws.cell(row=row_idx, column=program_col_idx).value).strip()
        term_cell_value = str(ws.cell(row=row_idx, column=term_col_idx).value).strip()

        if program_cell_value == "Total":
            try:
                ws.merge_cells(start_row=row_idx, end_row=row_idx, start_column=program_col_idx, end_column=degree_col_idx)
                ws.cell(row=row_idx, column=program_col_idx).alignment = align_center_center
            except Exception as e_merge_total:
                 logger.warning(f"Could not merge 'Total' label in row {row_idx} on sheet '{ws.title}': {e_merge_total}")
        if term_cell_value == "Grand Total":
            try:
                ws.merge_cells(start_row=row_idx, end_row=row_idx, start_column=term_col_idx, end_column=degree_col_idx)
                ws.cell(row=row_idx, column=term_col_idx).alignment = align_center_center
            except Exception as e_merge_grand:
                 logger.warning(f"Could not merge 'Grand Total' label in row {row_idx} on sheet '{ws.title}': {e_merge_grand}")
    logger.debug(f"'Total' and 'Grand Total' label cell merging complete for '{ws.title}'.")

    # --- 3. Apply Styles (Fonts, Fills, Borders, Alignment) ---
    # (This styling logic is generally applicable)
    logger.debug(f"Applying row/cell styles for '{ws.title}'...")
    header_row_idx = 1
    max_col_idx = ws.max_column

    for col_idx in range(1, max_col_idx + 1): # Style Header
        cell = ws.cell(row=header_row_idx, column=col_idx)
        cell.font = font_bold
        cell.alignment = align_center_center
        cell.border = Border(
            left=side_medium_black if col_idx == 1 else side_thin_white,
            right=side_medium_black if col_idx == max_col_idx else side_thin_white,
            top=side_medium_black,
            bottom=side_medium_black
        )

    is_even_data_row_group = True # For zebra striping
    for row_idx in range(2, ws.max_row + 1): # Style Data Rows
        current_row_fill = None
        current_row_font = Font()
        is_total_row_type = str(ws.cell(row=row_idx, column=program_col_idx).value).strip() == "Total"
        # Check the term column for "Grand Total" as that's where the label is placed for merged cells
        is_grand_total_row_type = str(ws.cell(row=row_idx, column=term_col_idx).value).strip() == "Grand Total"


        if is_grand_total_row_type:
            current_row_fill = fill_grand_total_row
            current_row_font = font_bold
        elif is_total_row_type:
            current_row_fill = fill_total_row
            current_row_font = font_bold
        else:
            if is_even_data_row_group:
                 current_row_fill = fill_alt_row
            is_even_data_row_group = not is_even_data_row_group

        for col_idx in range(1, max_col_idx + 1):
            cell = ws.cell(row=row_idx, column=col_idx)
            if current_row_fill:
                cell.fill = current_row_fill
            if current_row_font.bold:
                cell.font = current_row_font
            if col_idx >= first_data_col_idx: # Center align numeric/data columns
                cell.alignment = align_center_center

            border_left = side_thin_white
            border_right = side_thin_white
            border_top = side_thin_white
            border_bottom = side_thin_white
            if col_idx == 1: border_left = side_medium_black
            if col_idx == max_col_idx: border_right = side_medium_black
            if row_idx == ws.max_row: border_bottom = side_medium_black
            if is_total_row_type or is_grand_total_row_type:
                border_top = side_medium_black
                border_bottom = side_medium_black
            cell.border = Border(left=border_left, right=border_right, top=border_top, bottom=border_bottom)
    logger.debug(f"Row/cell styles applied for '{ws.title}'.")

    # --- 4. Refine borders for merged "Application Term" cells ---
    logger.debug(f"Refining borders for merged 'Application Term' cells for '{ws.title}'...")
    for merged_range_obj in ws.merged_cells.ranges:
        min_col, min_row, max_col, max_row_range = merged_range_obj.bounds # type: ignore
        if min_col == term_col_idx and max_col == term_col_idx: # Merged in App Term col
            for r_idx in range(min_row, max_row_range + 1):
                for c_idx in range(min_col, max_col +1):
                    cell = ws.cell(row=r_idx, column=c_idx)
                    current_border = cell.border.copy()
                    current_border.left = side_medium_black
                    current_border.right = side_medium_black
                    if r_idx == min_row: current_border.top = side_medium_black
                    if r_idx == max_row_range: current_border.bottom = side_medium_black
                    cell.border = current_border
    logger.debug(f"Borders for merged 'Application Term' cells refined for '{ws.title}'.")

    _auto_adjust_column_widths(ws)
    logger.info(f"Detailed styling for '{sheet_title_for_logging}' sheet '{ws.title}' complete.")


def format_excel_workbook(excel_path: str):
    logger.info(f"Starting Excel formatting for workbook: {excel_path}")
    try:
        wb = load_workbook(excel_path)
        for sheet_name in wb.sheetnames:
            ws = wb[sheet_name]
            logger.info(f"Formatting sheet: {sheet_name}")

            if sheet_name == settings.EXCEL_SHEET_NAME_PROGRESS_REPORT:
                _apply_detailed_report_styles(ws,
                                              settings.PROGRESS_REPORT_FINAL_COLUMN_ORDER,
                                              "Progress Report")
            elif sheet_name == settings.EXCEL_SHEET_NAME_ADMIT_BREAKDOWN: # <<< NEW ELIF
                _apply_detailed_report_styles(ws,
                                              settings.ADMIT_BREAKDOWN_FINAL_COLUMN_ORDER,
                                              "Admit Breakdown")
            elif sheet_name.startswith("ERROR_"):
                 _auto_adjust_column_widths(ws)
                 if ws.max_row >=1:
                    for cell in ws[1]:
                        cell.font = Font(bold=True, color="FF0000")
            else: # Generic formatting for other sheets (e.g., Raw Data)
                logger.info(f"Applying generic formatting to sheet: {sheet_name}")
                ws.freeze_panes = 'A2'
                logger.debug(f"Froze top row for sheet: {ws.title}")
                if ws.max_row >= 1:
                    for cell in ws[1]:
                        cell.font = Font(bold=settings.EXCEL_FONT_BOLD)
                        cell.alignment = Alignment(**settings.EXCEL_ALIGNMENT_CENTER)
                _auto_adjust_column_widths(ws)

        wb.save(excel_path)
        logger.info(f"✅ Excel workbook formatting complete. Saved to: {excel_path}")

    except FileNotFoundError:
        logger.error(f"Error: Excel file not found at '{excel_path}'.")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during Excel formatting of '{excel_path}': {e}", exc_info=True)
        raise