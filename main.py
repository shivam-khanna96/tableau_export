# tableau_admissions_report/main.py
import logging
import sys
import os
import pandas as pd

# --- Project-specific imports ---
try:
    from config import settings
except ValueError as config_error:
    print(f"CRITICAL CONFIGURATION ERROR: {config_error}", file=sys.stderr)
    print("Please ensure your .env file is correctly set up in the project root.", file=sys.stderr)
    sys.exit(1)

from tableau_connector.client import TableauClient, TableauAPIError
from report_processor import data_handler, excel_formatter
from email_sender import mailer


# --- ROBUST LOGGING SETUP ---
# This configuration captures ALL logs from ALL modules (main, client, mailer, etc.)
# and sends them to TWO places:
# 1. The Console (Standard Output)
# 2. The Daily Log File (settings.LOG_FILE_FULL_PATH)

logging.basicConfig(
    level=getattr(logging, settings.LOGGING_LEVEL.upper(), logging.INFO),
    format=settings.LOG_FORMAT,
    handlers=[
        # Handler 1: Console
        logging.StreamHandler(sys.stdout),
        
        # Handler 2: Daily Log File (mode='a' appends if run multiple times in one day)
        logging.FileHandler(settings.LOG_FILE_FULL_PATH, mode='a', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)


def run_reporting_workflow():
    """
    Orchestrates the entire workflow.
    """
    logger.info("="*60)
    logger.info(f"Starting Admissions Report Workflow - Date: {settings.TODAY_STR}")
    logger.info("="*60)
    logger.info(f"Log file location: {settings.LOG_FILE_FULL_PATH}")

    tableau_client = None 

    try:
        # --- 1. Initialize Tableau Client and Authenticate ---
        logger.info("Initializing Tableau Client...")
        tableau_client = TableauClient(
            server_url=settings.TABLEAU_SERVER,
            site_name=settings.TABLEAU_SITE,
            token_name=settings.TABLEAU_TOKEN_NAME,
            token_secret=settings.TABLEAU_TOKEN_SECRET,
            api_version=settings.TABLEAU_API_VERSION
        )
        tableau_client.authenticate()

        # --- 2. Find Matching Workbook(s) ---
        # ... [keep initialization and authentication code] ...

        # Helper function to fetch views for a given dashboard
        def fetch_dashboard_data(workbook_contains, view_urls, default_terms):
            dashboard_data = {}
            if not default_terms:
                return dashboard_data
                
            workbooks = tableau_client.find_matching_workbooks(settings.TARGET_PROJECT_NAME, workbook_contains)
            if not workbooks:
                logger.warning(f"No workbook found containing '{workbook_contains}'.")
                return dashboard_data

            wb_id = workbooks[0].get("id")
            target_urls = list(view_urls.values())
            views = tableau_client.find_matching_views(wb_id, target_urls)

            for view_key, target_url in view_urls.items():
                matched_view = next((v for v in views if v.get("viewUrlName") == target_url), None)
                if matched_view:
                    # Apply the specific override for admit_breakdown
                    view_terms = settings.ADMIT_BREAKDOWN_TERMS if view_key == "admit_breakdown" else default_terms
                    
                    logger.info(f"Fetching {view_key} from {workbook_contains} for terms: {view_terms}")
                    csv_bytes = tableau_client.get_view_data_csv(
                        matched_view.get("id"),
                        filter_name=settings.VIEW_FILTER_NAME,
                        filter_values=view_terms
                    )
                    
                    # --- NEW: Graceful Error Handling for Empty Files ---
                    try:
                        dashboard_data[view_key] = pd.read_csv(io.BytesIO(csv_bytes), thousands=',')
                    except pd.errors.EmptyDataError:
                        logger.error(f"CRITICAL: Tableau returned an empty file ({len(csv_bytes)} bytes) for '{view_key}'.")
                        logger.error("Skipping this view but continuing the workflow.")
                        dashboard_data[view_key] = pd.DataFrame() # Return empty DF so the script doesn't crash
                    # ----------------------------------------------------
                        
            return dashboard_data

        # --- 2 & 3. Fetch Data from Both Dashboards ---
        import io # Ensure 'import io' is at the top of main.py
        
        legacy_dataframes = fetch_dashboard_data(
            settings.LEGACY_WORKBOOK_NAME_CONTAINS, 
            settings.LEGACY_VIEW_URLS, 
            settings.LEGACY_TERMS
        )
        
        workday_dataframes = fetch_dashboard_data(
            settings.WORKDAY_WORKBOOK_NAME_CONTAINS, 
            settings.WORKDAY_VIEW_URLS, 
            settings.WORKDAY_TERMS
        )

        # --- 4. Generate Consolidated Excel Report ---
        logger.info(f"Preparing to generate Excel report at: {settings.OUTPUT_EXCEL_FULL_PATH}")
            
        with pd.ExcelWriter(settings.OUTPUT_EXCEL_FULL_PATH, engine="openpyxl") as writer:
            data_handler.generate_consolidated_report(
                legacy_data=legacy_dataframes,
                workday_data=workday_dataframes,
                excel_writer=writer
            )
        logger.info(f"Raw data written to Excel sheets in: {settings.OUTPUT_EXCEL_FULL_PATH}")

        # --- 5. Format the Excel Workbook ---
        logger.info("Applying formatting to the generated Excel workbook...")
        excel_formatter.format_excel_workbook(settings.OUTPUT_EXCEL_FULL_PATH)

        # --- 6. Send Email with the Report ---
        if settings.EMAIL_RECIPIENTS_LIST:
            logger.info(f"Preparing to send report via Graph API to: {', '.join(settings.EMAIL_RECIPIENTS_LIST)}")
            mailer.prepare_and_send_report_email(
                attachment_full_path=settings.OUTPUT_EXCEL_FULL_PATH
            )
        else:
            logger.info("No email recipients configured. Skipping email step.")

        logger.info("Workflow Completed Successfully!")

    except TableauAPIError as api_err:
        logger.critical(f"A Tableau API error occurred: {api_err}", exc_info=True)
    except ValueError as val_err: 
        logger.critical(f"A data or configuration value error occurred: {val_err}", exc_info=True)
    except IOError as io_err: 
        logger.critical(f"An I/O error occurred (e.g., writing Excel file): {io_err}", exc_info=True)
    except Exception as e:
        logger.critical(f"An unexpected error occurred in the main workflow: {e}", exc_info=True)
    finally:
        if tableau_client:
            logger.info("Attempting to sign out from Tableau...")
            tableau_client.sign_out()
        logger.info("Workflow finished.")
        logger.info("="*60 + "\n") # Visual separator in the log file

if __name__ == "__main__":
    run_reporting_workflow()