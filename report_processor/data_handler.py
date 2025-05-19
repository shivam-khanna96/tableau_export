# tableau_admissions_report/report_processor/data_handler.py
import pandas as pd
from io import BytesIO
import logging
from typing import Dict, Any, List, Optional

from config import settings # Import configurations
from tableau_connector.client import TableauClient # For type hinting

logger = logging.getLogger(__name__)

def _clean_progress_report_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans the initial DataFrame for the Progress Report.
    - Drops specified columns.
    - Removes rows where any cell's content is exactly settings.PROGRESS_REPORT_REMOVE_ROW_IF_CONTAINS_STRING (case-insensitive).
    """
    logger.debug("Initial Progress Report DataFrame shape: %s", df.shape)
    
    # Drop unwanted columns if they exist
    cols_to_drop = [col for col in settings.PROGRESS_REPORT_DROP_COLUMNS if col in df.columns]
    if cols_to_drop:
        df.drop(columns=cols_to_drop, inplace=True)
        logger.debug(f"Dropped columns: {cols_to_drop}. New shape: {df.shape}")

    # Remove rows if any cell *exactly* matches PROGRESS_REPORT_REMOVE_ROW_IF_CONTAINS_STRING (case-insensitive, ignoring surrounding whitespace)
    if settings.PROGRESS_REPORT_REMOVE_ROW_IF_CONTAINS_STRING:
        rows_before_filter = len(df)
        
        target_string_lower_stripped = settings.PROGRESS_REPORT_REMOVE_ROW_IF_CONTAINS_STRING.lower().strip()

        condition = df.apply(
            lambda row: row.astype(str).str.strip().str.lower().eq(target_string_lower_stripped).any(), 
            axis=1
        )
        
        df = df[~condition] 
        rows_after_filter = len(df)
        logger.debug(
            f"Filtered rows based on exact match to '{settings.PROGRESS_REPORT_REMOVE_ROW_IF_CONTAINS_STRING}'. "
            f"Total rows removed: {rows_before_filter - rows_after_filter}. New shape: {df.shape}"
        )
    return df

def _pivot_progress_report_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pivots the DataFrame for the Progress Report.
    """
    if df.empty:
        logger.warning("DataFrame is empty before pivoting. Returning empty DataFrame.")
        return pd.DataFrame(columns=settings.PROGRESS_REPORT_FINAL_COLUMN_ORDER)

    logger.debug("Pivoting Progress Report DataFrame. Input shape: %s", df.shape)
    
    pivot_df = df.pivot_table(
        index=settings.PROGRESS_REPORT_PIVOT_INDEX_COLUMNS,
        columns=settings.PROGRESS_REPORT_PIVOT_AGG_COLUMN,
        values=settings.PROGRESS_REPORT_PIVOT_VALUES_COLUMN,
        aggfunc="sum",
        fill_value=0 
    ).reset_index()
    logger.debug(f"Pivot table created. Shape: {pivot_df.shape}")

    for col in settings.PROGRESS_REPORT_FINAL_COLUMN_ORDER:
        if col not in pivot_df.columns:
            if col in settings.PROGRESS_REPORT_NUMERIC_COLUMNS_FOR_INT_CONVERSION:
                pivot_df[col] = 0
            else:
                pivot_df[col] = "" 

    pivot_df = pivot_df.reindex(columns=settings.PROGRESS_REPORT_FINAL_COLUMN_ORDER, fill_value=0)

    for col in settings.PROGRESS_REPORT_NUMERIC_COLUMNS_FOR_INT_CONVERSION:
        if col in pivot_df.columns:
            if pivot_df[col].dtype == 'object': 
                pivot_df[col] = pivot_df[col].astype(str).str.replace(',', '', regex=False)
            
            pivot_df[col] = pd.to_numeric(pivot_df[col], errors='coerce').fillna(0).astype(int) # Changed errors to 'coerce'
            logger.debug(f"Converted column '{col}' to numeric (int), handling commas.")
    
    logger.debug(f"Columns reordered and types converted. Final pivot shape before sort: {pivot_df.shape}")
    return pivot_df

def _add_subtotals_and_grandtotal_to_progress_report(pivot_df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds subtotal rows for each 'Application Term' and a grand total row.
    Sorts 'Application Term' by Year, then by custom Term order (Spring, Summer, Fall).
    """
    if pivot_df.empty:
        logger.warning("Pivot DataFrame is empty. Cannot add subtotals or grand total.")
        return pivot_df
    
    logger.debug("Adding subtotals and grand total to Progress Report. Input pivot_df shape: %s", pivot_df.shape)

    # --- Custom Sorting Logic for 'Application Term' ---
    if "Application Term" in pivot_df.columns:
        # Define the desired order of terms
        term_order_map = {'SPRING': 0, 'SUMMER': 1, 'FALL': 2}
        
        # Helper function to extract year and term name, and map term name to sort order
        def get_sort_keys(term_str):
            if pd.isna(term_str) or term_str.strip() == "" or term_str == "Grand Total": # Handle Grand Total and empty/NaN
                return (float('inf'), float('inf')) # Ensure Grand Total and blanks go to the end
            
            parts = term_str.upper().split()
            term_name = parts[0]
            year = float('inf') # Default for unparsable years
            try:
                if len(parts) > 1:
                    year = int(parts[-1]) # Assumes year is the last part
            except ValueError:
                logger.warning(f"Could not parse year from term: {term_str}. It will be sorted towards the end.")

            term_sort_order = term_order_map.get(term_name, float('inf')) # Unrecognized terms go to end
            return (year, term_sort_order)

        # Create temporary sort key columns
        # We apply this to a copy to avoid SettingWithCopyWarning if pivot_df is a slice
        pivot_df_copy = pivot_df.copy()
        sort_keys = pivot_df_copy["Application Term"].apply(get_sort_keys)
        pivot_df_copy['_Sort_Year'] = sort_keys.apply(lambda x: x[0])
        pivot_df_copy['_Sort_Term_Order'] = sort_keys.apply(lambda x: x[1])

        # Sort by the new keys, then by Program, then drop temporary sort keys
        pivot_df = pivot_df_copy.sort_values(
            by=['_Sort_Year', '_Sort_Term_Order', 'Program'],
            ascending=[True, True, True] # Year ascending, Term Order ascending, Program ascending
        ).drop(columns=['_Sort_Year', '_Sort_Term_Order'])
        
        logger.debug(f"Custom sorted pivot_df by Application Term (Year, Custom Term Order), then Program. Shape: {pivot_df.shape}")
        logger.debug(f"Order of 'Application Term' after sort: {pivot_df['Application Term'].unique().tolist()}")
    else:
        logger.warning("'Application Term' column not found in pivot_df. Skipping custom sort.")
        # Fallback to original sort if 'Application Term' is missing, though it's a key column
        pivot_df = pivot_df.sort_values(by=["Program"], key=lambda col: col.astype(str))


    all_rows_with_subtotals = []
    numeric_cols_for_sum = [
        col for col in settings.PROGRESS_REPORT_SUBTOTAL_COLUMNS_TO_AGGREGATE if col in pivot_df.columns
    ]

    # Group by the now correctly sorted "Application Term"
    # sort=False in groupby preserves the order from the DataFrame if it's already sorted.
    grouped_by_term = pivot_df.groupby("Application Term", sort=False) 
    logger.debug(f"Grouping pivot_df by 'Application Term'. Number of groups: {len(grouped_by_term)}. Group names (in order): {list(grouped_by_term.groups.keys())}")

    for term, group in grouped_by_term:
        # Skip adding subtotals for "Grand Total" if it somehow became a group name here
        if term == "Grand Total": 
            all_rows_with_subtotals.append(group) # Append Grand Total rows if they exist from pivot
            continue

        logger.debug(f"Processing group for term: '{term}'. Group shape: {group.shape}")
        all_rows_with_subtotals.append(group)
        
        subtotal_data = {col: group[col].sum() for col in numeric_cols_for_sum}
        subtotal_row = pd.DataFrame([subtotal_data])
        subtotal_row["Application Term"] = term 
        subtotal_row["Program"] = "Total"
        for col in settings.PROGRESS_REPORT_PIVOT_INDEX_COLUMNS:
            if col not in ["Application Term", "Program"] and col not in subtotal_row.columns:
                subtotal_row[col] = ""
        
        for col in pivot_df.columns:
            if col not in subtotal_row:
                 subtotal_row[col] = "" 
        all_rows_with_subtotals.append(subtotal_row[pivot_df.columns]) 

    if not all_rows_with_subtotals: 
        logger.warning("No rows generated after subtotal processing. Returning original pivot_df.")
        return pivot_df

    final_df_with_subtotals = pd.concat(all_rows_with_subtotals, ignore_index=True)
    logger.debug(f"DataFrame with subtotals created. Shape: {final_df_with_subtotals.shape}")

    # Add Grand Total row
    # This logic assumes "Grand Total" is not already present as an "Application Term" in pivot_df
    # It calculates grand total based on the original data rows in pivot_df (before subtotals were added)
    if not pivot_df[pivot_df["Application Term"] == "Grand Total"].empty:
        logger.debug("Grand Total row seems to already exist in pivot_df. Skipping recalculation of grand total from scratch.")
    elif not final_df_with_subtotals.empty and numeric_cols_for_sum:
        # Calculate grand total only from original data rows (not from 'Total' program rows)
        # We use pivot_df here as it contains only the original data rows before any subtotals were added
        grand_total_data = {col: pivot_df[col].sum() for col in numeric_cols_for_sum}

        grand_total_row = pd.DataFrame([grand_total_data]) 
        grand_total_row["Application Term"] = "Grand Total"
        for col in settings.PROGRESS_REPORT_PIVOT_INDEX_COLUMNS:
             if col != "Application Term" and col not in grand_total_row.columns: 
                grand_total_row[col] = ""
        
        for col in final_df_with_subtotals.columns:
            if col not in grand_total_row:
                grand_total_row[col] = "" 
        final_df_with_subtotals = pd.concat([final_df_with_subtotals, grand_total_row[final_df_with_subtotals.columns]], ignore_index=True)
        logger.debug(f"Grand total added. Final DataFrame shape: {final_df_with_subtotals.shape}")
    
    return final_df_with_subtotals

def process_progress_report_data(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Orchestrates the full processing for the 'Progress Report' data.
    """
    logger.info("Starting processing for 'Progress Report' data...")
    if 'Application Term' in df_raw.columns:
        logger.debug(f"Raw data for Progress Report. Shape: {df_raw.shape}. Unique terms: {df_raw['Application Term'].unique()}")
    else:
        logger.warning("Raw data for Progress Report is missing 'Application Term' column.")

    df_cleaned = _clean_progress_report_dataframe(df_raw.copy())
    if df_cleaned.empty:
        logger.warning("Progress report data is empty after cleaning. No further processing will occur.")
        return pd.DataFrame(columns=settings.PROGRESS_REPORT_FINAL_COLUMN_ORDER) 

    df_pivoted = _pivot_progress_report_dataframe(df_cleaned)
    if df_pivoted.empty:
        logger.warning("Progress report data is empty after pivoting. No subtotals will be added.")
        return df_pivoted 

    final_df = _add_subtotals_and_grandtotal_to_progress_report(df_pivoted)
    
    logger.info("'Progress Report' data processing complete.")
    return final_df

def process_raw_data_applicant_download(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Processes data for the 'PowerCampusApplicantDownload' (Raw Data) view.
    """
    logger.info("Starting processing for 'Raw Data Applicant Download'...")
    df = df_raw.copy()
    logger.debug(f"Initial Raw Data DataFrame shape: {df.shape}")

    cols_to_drop = [col for col in settings.RAW_DATA_DROP_COLUMNS if col in df.columns]
    if cols_to_drop:
        df.drop(columns=cols_to_drop, inplace=True, errors='ignore')
        logger.debug(f"Dropped columns: {cols_to_drop}. New shape: {df.shape}")

    existing_cols_for_selection = [col for col in settings.RAW_DATA_FINAL_COLUMN_SELECTION_ORDER if col in df.columns]
    if not existing_cols_for_selection:
        logger.warning("No columns from RAW_DATA_FINAL_COLUMN_SELECTION_ORDER exist in the DataFrame. Returning as is after drops.")
    else:
        df = df[existing_cols_for_selection]
        logger.debug(f"Selected and reordered columns. Final shape: {df.shape}")
    
    logger.info("'Raw Data Applicant Download' processing complete.")
    return df


def generate_excel_sheets_from_views(
    tableau_client: TableauClient,
    views_to_fetch: List[Dict[str, Any]],
    excel_writer: pd.ExcelWriter
) -> None:
    """
    Fetches data for specified Tableau views, processes it according to view type,
    and writes each processed DataFrame to a sheet in the provided ExcelWriter.
    """
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

        try:
            csv_data_bytes = tableau_client.get_view_data_csv(
                view_id,
                filter_name=settings.VIEW_FILTER_NAME,
                filter_values=settings.VIEW_FILTER_VALUES
            )
            # Consider adding thousands=',' to read_csv if numbers with commas are directly in CSV
            # and not just a result of pivoting.
            raw_df = pd.read_csv(BytesIO(csv_data_bytes)) 
            logger.info(f"Successfully fetched and read CSV for view '{view_display_name}'. Shape: {raw_df.shape}")
            if logger.isEnabledFor(logging.DEBUG) and 'Application Term' in raw_df.columns: 
                 logger.debug(f"Unique 'Application Term' values in raw CSV for '{view_display_name}': {raw_df['Application Term'].unique()}")


            processed_df = None
            if view_url_name == settings.PROGRESS_REPORT_VIEW_URL_NAME:
                processed_df = process_progress_report_data(raw_df)
            elif view_url_name == settings.RAW_DATA_VIEW_URL_NAME:
                processed_df = process_raw_data_applicant_download(raw_df)
            else:
                logger.warning(f"No specific data processing logic defined for view URL name: '{view_url_name}'. Writing raw data.")
                processed_df = raw_df 

            if processed_df is not None and not processed_df.empty:
                processed_df.to_excel(excel_writer, sheet_name=sheet_name, index=False)
                logger.info(f"Successfully wrote sheet '{sheet_name}' for view '{view_display_name}'.")
            elif processed_df is not None and processed_df.empty:
                 logger.warning(f"Processed DataFrame for view '{view_display_name}' is empty. Sheet '{sheet_name}' will be empty.")
                 pd.DataFrame().to_excel(excel_writer, sheet_name=sheet_name, index=False) 

        except Exception as e: 
            logger.error(f"Failed to fetch, process, or write data for view '{view_display_name}' (ID: {view_id}): {e}", exc_info=True)
            error_df = pd.DataFrame([{"Error": f"Could not load/process data for view {view_display_name}: {str(e)}"}])
            error_sheet_name = f"ERROR_{sheet_name[:25]}" 
            error_df.to_excel(excel_writer, sheet_name=error_sheet_name, index=False)

    logger.info("All specified views have been processed for Excel sheet generation.")

