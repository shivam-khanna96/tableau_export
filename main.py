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
        matching_workbooks = tableau_client.find_matching_workbooks(
            project_name=settings.TARGET_PROJECT_NAME,
            name_contains_filter=settings.TARGET_WORKBOOK_NAME_CONTAINS
        )

        if not matching_workbooks:
            logger.warning(
                f"No workbooks found matching project '{settings.TARGET_PROJECT_NAME}' "
                f"and name containing '{settings.TARGET_WORKBOOK_NAME_CONTAINS}'. Workflow cannot proceed."
            )
            return

        target_workbook = matching_workbooks[0]
        target_workbook_id = target_workbook.get("id")
        logger.info(f"Using workbook: '{target_workbook.get('name')}' (ID: {target_workbook_id})")

        if not target_workbook_id:
            logger.error("Selected workbook has no ID. Cannot proceed.")
            return

        # --- 3. Find Matching Views ---
        views_to_process_details = tableau_client.find_matching_views(
            workbook_id=target_workbook_id,
            target_view_url_names=settings.TARGET_VIEW_URL_NAMES
        )

        if not views_to_process_details:
            logger.warning(
                f"No views found matching the target URL names {settings.TARGET_VIEW_URL_NAMES} "
                f"in workbook ID '{target_workbook_id}'. Workflow cannot proceed."
            )
            return

        logger.info(f"Found {len(views_to_process_details)} views to process.")

        # --- 4. Generate Excel Report ---
        logger.info(f"Preparing to generate Excel report at: {settings.OUTPUT_EXCEL_FULL_PATH}")
            
        with pd.ExcelWriter(settings.OUTPUT_EXCEL_FULL_PATH, engine="openpyxl") as writer:
            data_handler.generate_excel_sheets_from_views(
                tableau_client=tableau_client,
                views_to_fetch=views_to_process_details,
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