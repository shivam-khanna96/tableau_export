# tableau_admissions_report/report_processor/data_handler.py
import pandas as pd
from io import BytesIO
import logging
from typing import Dict, Any, List, Optional

from config import settings # Import configurations
from tableau_connector.client import TableauClient # For type hinting

logger = logging.getLogger(__name__)

# --- Helper function (can remain mostly the same, or be made more generic if needed) ---
# _clean_dataframe, _pivot_dataframe, _add_subtotals_and_grandtotal
# For simplicity, we'll reuse/adapt them by passing the correct settings constants.
# The original _clean_progress_report_dataframe and _pivot_progress_report_dataframe
# can be made more generic if they only differ by the constants they use.
# Let's assume for now we create specific versions or make them more adaptable.

def _clean_dataframe(df: pd.DataFrame, drop_cols: List[str], remove_row_string: Optional[str]) -> pd.DataFrame:
    """Generic helper to clean DataFrame."""
    logger.debug("Initial DataFrame shape for cleaning: %s", df.shape)
    cols_to_drop_existing = [col for col in drop_cols if col in df.columns]
    if cols_to_drop_existing:
        df.drop(columns=cols_to_drop_existing, inplace=True)
        logger.debug(f"Dropped columns: {cols_to_drop_existing}. New shape: {df.shape}")

    if remove_row_string:
        rows_before_filter = len(df)
        target_string_lower_stripped = remove_row_string.lower().strip()
        condition = df.apply(
            lambda row: row.astype(str).str.strip().str.lower().eq(target_string_lower_stripped).any(),
            axis=1
        )
        df = df[~condition]
        rows_after_filter = len(df)
        logger.debug(
            f"Filtered rows based on exact match to '{remove_row_string}'. "
            f"Rows removed: {rows_before_filter - rows_after_filter}. New shape: {df.shape}"
        )
    return df

def _pivot_dataframe(df: pd.DataFrame, index_cols: List[str], agg_col: str, values_col: str,
                     final_col_order: List[str], numeric_cols: List[str]) -> pd.DataFrame:
    """Generic helper to pivot DataFrame."""
    if df.empty:
        logger.warning("DataFrame is empty before pivoting. Returning empty DataFrame.")
        return pd.DataFrame(columns=final_col_order)

    logger.debug("Pivoting DataFrame. Input shape: %s", df.shape)
    # Check if pivoting is actually needed (i.e., if agg_col and values_col are present)
    if agg_col not in df.columns or values_col not in df.columns:
        logger.info(f"Pivot columns '{agg_col}' or '{values_col}' not found. Assuming data is already shaped. Proceeding with reordering and type conversion.")
        pivot_df = df.copy()
    else:
        pivot_df = df.pivot_table(
            index=index_cols,
            columns=agg_col,
            values=values_col,
            aggfunc="sum", # Or 'first' if values are already aggregated by Tableau
            fill_value=0
        ).reset_index()
        logger.debug(f"Pivot table created. Shape: {pivot_df.shape}")

    # Ensure all final columns exist, filling with 0 for numeric or "" for others
    for col in final_col_order:
        if col not in pivot_df.columns:
            if col in numeric_cols:
                pivot_df[col] = 0
            else:
                pivot_df[col] = "" # For non-numeric columns like CURRICULUM, DEGREE if they become part of pivot target

    pivot_df = pivot_df.reindex(columns=final_col_order, fill_value=0) # fill_value=0 might be an issue for string columns

    for col in numeric_cols:
        if col in pivot_df.columns:
            if pivot_df[col].dtype == 'object':
                pivot_df[col] = pivot_df[col].astype(str).str.replace(',', '', regex=False)
            pivot_df[col] = pd.to_numeric(pivot_df[col], errors='coerce').fillna(0).astype(int)
            logger.debug(f"Converted column '{col}' to numeric (int), handling commas.")

    logger.debug(f"Columns reordered and types converted. Final pivot shape before sort: {pivot_df.shape}")
    return pivot_df

def _add_subtotals_and_grandtotal(pivot_df: pd.DataFrame,
                                  index_cols_for_subtotal: List[str], # e.g., ["Application Term", "Program"]
                                  subtotal_cols_to_agg: List[str],
                                  report_specific_final_col_order: List[str]) -> pd.DataFrame:
    """
    Generic helper to adds subtotal rows for each 'Application Term' and a grand total row.
    Sorts 'Application Term' by Year, then by custom Term order (Spring, Summer, Fall).
    """
    if pivot_df.empty:
        logger.warning("Pivot DataFrame is empty. Cannot add subtotals or grand total.")
        return pivot_df

    logger.debug("Adding subtotals and grand total. Input pivot_df shape: %s", pivot_df.shape)

    # --- Custom Sorting Logic for 'Application Term' ---
    if "Application Term" in pivot_df.columns:
        term_order_map = {'SPRING': 0, 'SUMMER': 1, 'FALL': 2}
        def get_sort_keys(term_str):
            if pd.isna(term_str) or term_str.strip() == "" or term_str == "Grand Total":
                return (float('inf'), float('inf'))
            parts = term_str.upper().split()
            term_name = parts[0]
            year = float('inf')
            try:
                if len(parts) > 1:
                    year = int(parts[-1])
            except ValueError:
                logger.warning(f"Could not parse year from term: {term_str}.")
            term_sort_order = term_order_map.get(term_name, float('inf'))
            return (year, term_sort_order)

        pivot_df_copy = pivot_df.copy()
        sort_keys_series = pivot_df_copy["Application Term"].apply(get_sort_keys)
        pivot_df_copy['_Sort_Year'] = sort_keys_series.apply(lambda x: x[0])
        pivot_df_copy['_Sort_Term_Order'] = sort_keys_series.apply(lambda x: x[1])

        # Program might not always be an index column after pivoting for all reports,
        # but it's a common secondary sort key.
        sort_by_cols = ['_Sort_Year', '_Sort_Term_Order']
        if "Program" in pivot_df_copy.columns: # Check if "Program" is a column to sort by
             sort_by_cols.append('Program')

        pivot_df = pivot_df_copy.sort_values(
            by=sort_by_cols,
            ascending=[True, True, True] if "Program" in sort_by_cols else [True, True]
        ).drop(columns=['_Sort_Year', '_Sort_Term_Order'])
        logger.debug(f"Custom sorted pivot_df by Application Term (Year, Custom Term Order), then Program. Shape: {pivot_df.shape}")
    else:
        logger.warning("'Application Term' column not found. Skipping custom sort based on it.")
        if "Program" in pivot_df.columns: # Fallback sort if possible
            pivot_df = pivot_df.sort_values(by=["Program"], key=lambda col: col.astype(str))


    all_rows_with_subtotals = []
    numeric_cols_for_sum = [
        col for col in subtotal_cols_to_agg if col in pivot_df.columns
    ]

    # Ensure pivot_df has the correct final column order before processing
    pivot_df = pivot_df.reindex(columns=report_specific_final_col_order, fill_value=0)


    grouped_by_term = pivot_df.groupby("Application Term", sort=False)
    logger.debug(f"Grouping pivot_df by 'Application Term'. Number of groups: {len(grouped_by_term)}. Group names: {list(grouped_by_term.groups.keys())}")

    for term, group in grouped_by_term:
        if term == "Grand Total":
            all_rows_with_subtotals.append(group)
            continue

        logger.debug(f"Processing group for term: '{term}'. Group shape: {group.shape}")
        all_rows_with_subtotals.append(group)

        subtotal_data = {col: group[col].sum() for col in numeric_cols_for_sum}
        subtotal_row_df = pd.DataFrame([subtotal_data])
        subtotal_row_df["Application Term"] = term
        subtotal_row_df["Program"] = "Total"

        # Fill missing index columns (like CURRICULUM, DEGREE) with "" for subtotal rows
        for col in index_cols_for_subtotal: # e.g. ["Application Term", "Program", "CURRICULUM", "DEGREE"]
            if col not in ["Application Term", "Program"] and col not in subtotal_row_df.columns:
                subtotal_row_df[col] = ""

        # Ensure all columns from original pivot_df are present in subtotal_row_df, fill with "" if not a sum
        for col in pivot_df.columns:
            if col not in subtotal_row_df:
                 subtotal_row_df[col] = "" # Or 0 if it's expected to be numeric but not summed

        all_rows_with_subtotals.append(subtotal_row_df[pivot_df.columns]) # Ensure column order

    if not all_rows_with_subtotals:
        logger.warning("No rows generated after subtotal processing. Returning original pivot_df.")
        return pivot_df.reindex(columns=report_specific_final_col_order, fill_value=0)

    final_df_with_subtotals = pd.concat(all_rows_with_subtotals, ignore_index=True)
    logger.debug(f"DataFrame with subtotals created. Shape: {final_df_with_subtotals.shape}")

    # Add Grand Total row if not already present from pivot
    if not pivot_df[pivot_df["Application Term"] == "Grand Total"].empty:
        logger.debug("Grand Total row seems to already exist in pivot_df. Skipping recalculation.")
    elif not final_df_with_subtotals.empty and numeric_cols_for_sum:
        grand_total_data = {col: pivot_df[col].sum() for col in numeric_cols_for_sum} # Sum from original data
        grand_total_row_df = pd.DataFrame([grand_total_data])
        grand_total_row_df["Application Term"] = "Grand Total"

        for col in index_cols_for_subtotal:
             if col != "Application Term" and col not in grand_total_row_df.columns:
                grand_total_row_df[col] = ""
        for col in final_df_with_subtotals.columns:
            if col not in grand_total_row_df:
                grand_total_row_df[col] = ""

        final_df_with_subtotals = pd.concat(
            [final_df_with_subtotals, grand_total_row_df[final_df_with_subtotals.columns]],
            ignore_index=True
        )
        logger.debug(f"Grand total added. Final DataFrame shape: {final_df_with_subtotals.shape}")
    
    # Ensure final column order again
    return final_df_with_subtotals.reindex(columns=report_specific_final_col_order, fill_value=0)


def process_progress_report_data(df_raw: pd.DataFrame) -> pd.DataFrame:
    logger.info("Starting processing for 'Progress Report' data...")
    df_cleaned = _clean_dataframe(df_raw.copy(),
                                  settings.PROGRESS_REPORT_DROP_COLUMNS,
                                  settings.PROGRESS_REPORT_REMOVE_ROW_IF_CONTAINS_STRING)
    if df_cleaned.empty:
        logger.warning("Progress report data is empty after cleaning.")
        return pd.DataFrame(columns=settings.PROGRESS_REPORT_FINAL_COLUMN_ORDER)

    df_pivoted = _pivot_dataframe(df_cleaned,
                                  settings.PROGRESS_REPORT_PIVOT_INDEX_COLUMNS,
                                  settings.PROGRESS_REPORT_PIVOT_AGG_COLUMN,
                                  settings.PROGRESS_REPORT_PIVOT_VALUES_COLUMN,
                                  settings.PROGRESS_REPORT_FINAL_COLUMN_ORDER,
                                  settings.PROGRESS_REPORT_NUMERIC_COLUMNS_FOR_INT_CONVERSION)
    if df_pivoted.empty:
        logger.warning("Progress report data is empty after pivoting.")
        return df_pivoted

    final_df = _add_subtotals_and_grandtotal(df_pivoted,
                                             settings.PROGRESS_REPORT_PIVOT_INDEX_COLUMNS,
                                             settings.PROGRESS_REPORT_SUBTOTAL_COLUMNS_TO_AGGREGATE,
                                             settings.PROGRESS_REPORT_FINAL_COLUMN_ORDER)
    logger.info("'Progress Report' data processing complete.")
    return final_df

# <<< NEW FUNCTION for Admit Breakdown >>>
def process_admit_breakdown_data(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Orchestrates the full processing for the 'Admit Breakdown' data.
    """
    logger.info("Starting processing for 'Admit Breakdown' data...")
    # If the raw data for Admit Breakdown is already in the final shape (not needing pivot from Measure Names/Values)
    # the _pivot_dataframe step will handle it by just reordering and type converting.
    # Ensure ADMIT_BREAKDOWN_PIVOT_AGG_COLUMN and ADMIT_BREAKDOWN_PIVOT_VALUES_COLUMN are set appropriately
    # in settings.py. If no pivot is needed, they can be empty strings or names of non-existent columns.

    df_cleaned = _clean_dataframe(df_raw.copy(),
                                  settings.ADMIT_BREAKDOWN_DROP_COLUMNS,
                                  settings.ADMIT_BREAKDOWN_REMOVE_ROW_IF_CONTAINS_STRING)
    if df_cleaned.empty:
        logger.warning("Admit Breakdown data is empty after cleaning.")
        return pd.DataFrame(columns=settings.ADMIT_BREAKDOWN_FINAL_COLUMN_ORDER)

    df_processed = _pivot_dataframe(df_cleaned, # Renamed from df_pivoted to df_processed as pivot might be skipped
                                   settings.ADMIT_BREAKDOWN_PIVOT_INDEX_COLUMNS,
                                   settings.ADMIT_BREAKDOWN_PIVOT_AGG_COLUMN,
                                   settings.ADMIT_BREAKDOWN_PIVOT_VALUES_COLUMN,
                                   settings.ADMIT_BREAKDOWN_FINAL_COLUMN_ORDER,
                                   settings.ADMIT_BREAKDOWN_NUMERIC_COLUMNS_FOR_INT_CONVERSION)
    if df_processed.empty:
        logger.warning("Admit Breakdown data is empty after pivoting/processing.")
        return df_processed

    # Since Admit Breakdown is for a single term, the subtotal/grandtotal logic might simplify.
    # The generic _add_subtotals_and_grandtotal should still work:
    # - It groups by "Application Term". With one term, it will be one group.
    # - It adds a "Total" for that term.
    # - It adds a "Grand Total" which will be the same as that term's total if only one term is present in data.
    final_df = _add_subtotals_and_grandtotal(df_processed,
                                             settings.ADMIT_BREAKDOWN_PIVOT_INDEX_COLUMNS,
                                             settings.ADMIT_BREAKDOWN_SUBTOTAL_COLUMNS_TO_AGGREGATE,
                                             settings.ADMIT_BREAKDOWN_FINAL_COLUMN_ORDER)

    logger.info("'Admit Breakdown' data processing complete.")
    return final_df


def process_raw_data_applicant_download(df_raw: pd.DataFrame) -> pd.DataFrame:
    logger.info("Starting processing for 'Raw Data Applicant Download'...")
    df = df_raw.copy()
    cols_to_drop = [col for col in settings.RAW_DATA_DROP_COLUMNS if col in df.columns]
    if cols_to_drop:
        df.drop(columns=cols_to_drop, inplace=True, errors='ignore')
    existing_cols_for_selection = [col for col in settings.RAW_DATA_FINAL_COLUMN_SELECTION_ORDER if col in df.columns]
    if not existing_cols_for_selection:
        logger.warning("No columns from RAW_DATA_FINAL_COLUMN_SELECTION_ORDER exist. Returning as is after drops.")
    else:
        df = df[existing_cols_for_selection]
    logger.info("'Raw Data Applicant Download' processing complete.")
    return df


def generate_excel_sheets_from_views(
    tableau_client: TableauClient,
    views_to_fetch: List[Dict[str, Any]],
    excel_writer: pd.ExcelWriter
) -> None:
    logger.info(f"Processing {len(views_to_fetch)} views for Excel report generation...")

    for view_details in views_to_fetch:
        view_id = view_details.get("id")
        view_url_name = view_details.get("viewUrlName")
        view_display_name = view_details.get("name", view_url_name)

        if not view_id or not view_url_name:
            logger.warning(f"Skipping view due to missing ID or URLName: {view_details}")
            continue

        sheet_name = settings.VIEW_URL_NAME_TO_SHEET_NAME_MAP.get(view_url_name, view_url_name[:31])
        logger.info(f"Processing view: '{view_display_name}' (ID: {view_id}) for Excel sheet: '{sheet_name}'")

        # <<< MODIFIED: Determine filter values based on view >>>
        current_filter_values = settings.VIEW_FILTER_VALUES_MULTI_TERM # Default to multi-term
        if view_url_name == settings.ADMIT_BREAKDOWN_VIEW_URL_NAME:
            current_filter_values = settings.VIEW_FILTER_VALUES_SUMMER_ONLY
        # You might want to add similar logic if Raw Data also needs specific filters

        try:
            csv_data_bytes = tableau_client.get_view_data_csv(
                view_id,
                filter_name=settings.VIEW_FILTER_NAME,
                filter_values=current_filter_values # <<< MODIFIED: Use determined filter values
            )
            raw_df = pd.read_csv(BytesIO(csv_data_bytes))
            logger.info(f"Successfully fetched CSV for view '{view_display_name}'. Shape: {raw_df.shape}")

            processed_df = None
            if view_url_name == settings.PROGRESS_REPORT_VIEW_URL_NAME:
                processed_df = process_progress_report_data(raw_df)
            elif view_url_name == settings.ADMIT_BREAKDOWN_VIEW_URL_NAME: # <<< NEW ELIF
                processed_df = process_admit_breakdown_data(raw_df)
            elif view_url_name == settings.RAW_DATA_VIEW_URL_NAME:
                processed_df = process_raw_data_applicant_download(raw_df)
            else:
                logger.warning(f"No specific processing logic for view URL name: '{view_url_name}'. Writing raw data.")
                processed_df = raw_df

            if processed_df is not None and not processed_df.empty:
                processed_df.to_excel(excel_writer, sheet_name=sheet_name, index=False)
                logger.info(f"Successfully wrote sheet '{sheet_name}' for view '{view_display_name}'.")
            elif processed_df is not None and processed_df.empty:
                 logger.warning(f"Processed DataFrame for view '{view_display_name}' is empty. Sheet '{sheet_name}' will be empty.")
                 pd.DataFrame().to_excel(excel_writer, sheet_name=sheet_name, index=False)

        except Exception as e:
            logger.error(f"Failed to process/write data for view '{view_display_name}': {e}", exc_info=True)
            error_df = pd.DataFrame([{"Error": f"Could not load/process data for view {view_display_name}: {str(e)}"}])
            error_sheet_name = f"ERROR_{sheet_name[:25]}"
            error_df.to_excel(excel_writer, sheet_name=error_sheet_name, index=False)

    logger.info("All specified views processed for Excel sheet generation.")