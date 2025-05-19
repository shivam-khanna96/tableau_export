# Tableau Admissions Report Automator

This Python project automates the process of fetching admissions data from specified Tableau views, processing this data into structured reports, generating a formatted Excel file, and emailing this report to a list of recipients using Microsoft Outlook.

## Features

* **Secure Tableau Authentication:** Connects to Tableau Server using Personal Access Tokens (PATs). Credentials are managed via a `.env` file.
* **Targeted Data Extraction:**
    * Identifies specific workbooks based on project name and keywords in the workbook name.
    * Selects particular views within the chosen workbook using their stable `viewUrlName`.
    * Applies predefined filters (e.g., "Application Term") when fetching view data.
* **Data Processing with Pandas:**
    * **Progress Report:** Transforms raw data by dropping irrelevant columns, filtering out summary rows (e.g., rows containing "All"), pivoting the data, reordering columns, converting data types, and calculating/inserting subtotals for each "Application Term" and a final grand total.
    * **Raw Data Download:** Cleans the data by dropping specified columns and selecting/reordering the essential columns for a raw data export.
* **Advanced Excel Formatting with Openpyxl:**
    * Creates multi-sheet Excel workbooks.
    * Applies sophisticated formatting to the "Progress Report" sheet, including:
        * Merging cells for "Application Term" groups and "Total"/"Grand Total" labels.
        * Applying distinct fonts, background fills (zebra striping, total highlighting), and borders.
        * Centering text and numbers appropriately.
    * Auto-adjusts column widths for optimal readability across all sheets.
    * Provides basic header styling for other sheets.
* **Automated Emailing via Outlook:**
    * Sends the generated Excel report as an attachment.
    * Email recipients, subject, and body are configurable.
* **Configuration-Driven:**
    * Sensitive information (Tableau PAT secret, email lists) is stored in a `.env` file (not committed to version control).
    * Non-sensitive parameters (Tableau server details, target view/workbook names, column mappings, filter values, file paths) are managed in `config/settings.py`.
* **Structured & Maintainable Code:**
    * Follows a modular design with clear separation of concerns (Tableau connection, data processing, Excel formatting, emailing, configuration).
    * Includes comprehensive logging for diagnostics and monitoring.
    * Designed for readability, scalability, and adherence to Python best practices.

## Prerequisites

* Python 3.7+
* Microsoft Outlook application installed and configured on the machine running the script (if using the email feature).
* Access to a Tableau Server.
* A Tableau Personal Access Token (PAT) with the necessary permissions to access the required workbooks and views.

## Setup Instructions

1.  **Clone the Repository:**
    ```bash
    git clone <your-github-repository-url>
    cd tableau_admissions_report
    ```

2.  **Create and Activate a Virtual Environment (Recommended):**
    ```bash
    # For Windows
    python -m venv venv
    .\venv\Scripts\activate

    # For macOS/Linux
    python3 -m venv venv
    source venv/bin/activate
    ```

3.  **Install Dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Configure Environment Variables:**
    * Locate the `.env.example` file in the project root.
    * Create a copy of it and name it `.env`:
        ```bash
        cp .env.example .env
        ```
    * Open the `.env` file with a text editor and **fill in your actual credentials and configurations**:
        * `TABLEAU_SERVER`: Your Tableau server URL (e.g., `https://10ay.online.tableau.com`).
        * `TABLEAU_SITE`: Your Tableau site's content URL (e.g., `samuelmerritt`). Leave empty if using the default site.
        * `TABLEAU_TOKEN_NAME`: The name of your Tableau Personal Access Token.
        * `TABLEAU_TOKEN_SECRET`: The secret value of your Tableau PAT.
        * `TABLEAU_API_VERSION`: (Optional, defaults to "3.19") The Tableau API version your server uses.
        * `OUTPUT_DIR`: (Optional, defaults to "output_reports") The name of the folder where generated reports will be saved (relative to the project root).
        * `EMAIL_RECIPIENTS`: A semicolon-separated list of email addresses (e.g., `user1@example.com;user2@example.com`).
    * **IMPORTANT:** The `.env` file contains sensitive information and is explicitly ignored by Git (via `.gitignore`). **Never commit your `.env` file to any version control system.**

5.  **Review and Customize Application Settings (Optional):**
    * Open `config/settings.py`.
    * Review and adjust variables like `TARGET_PROJECT_NAME`, `TARGET_WORKBOOK_NAME_CONTAINS`, `TARGET_VIEW_URL_NAMES`, `VIEW_URL_NAME_TO_SHEET_NAME_MAP`, `VIEW_FILTER_NAME`, `VIEW_FILTER_VALUES`, and various data processing constants (column names, etc.) to match your specific Tableau setup and reporting requirements.

## Running the Script

Once the setup and configuration are complete, you can run the main automation script from the project's root directory (`tableau_admissions_report/`):

```bash
python main.py
```

The script will perform the following actions:
1.  Log its progress to the console (and optionally to a file if configured).
2.  Authenticate to the Tableau Server.
3.  Locate the specified workbook and views.
4.  Fetch data for each view, applying any configured filters.
5.  Process the retrieved data.
6.  Generate an Excel file with multiple sheets in the configured output directory (e.g., `output_reports/`).
7.  Apply detailed formatting to the Excel sheets.
8.  If email recipients are configured, send the generated Excel report as an attachment via Outlook.
9.  Sign out from the Tableau Server.

## Project Structure Overview

```
tableau_admissions_report/
├── main.py                     # Main orchestrator script
├── tableau_connector/          # For Tableau API communication
│   ├── __init__.py
│   └── client.py
├── report_processor/           # For data manipulation and Excel formatting
│   ├── __init__.py
│   ├── data_handler.py
│   └── excel_formatter.py
├── email_sender/               # For sending emails via Outlook
│   ├── __init__.py
│   └── mailer.py
├── config/                     # For application configurations
│   ├── __init__.py
│   └── settings.py
├── .env                        # (Local) Stores sensitive credentials - NOT IN GIT
├── .env.example                # Template for .env
├── .gitignore                  # Specifies files for Git to ignore
├── README.md                   # This documentation file
└── requirements.txt            # Python package dependencies
```

## Troubleshooting & Notes

* **Outlook Not Accessible:** If the script has trouble sending emails, ensure Outlook is running and configured. If running as a scheduled task or service, Outlook's COM automation might be restricted due to session isolation. For unattended execution, consider using `smtplib` with an SMTP server instead of Outlook automation.
* **Permissions:** Ensure the script has write permissions to the `OUTPUT_DIR` and read permissions for any necessary Tableau resources.
* **Tableau PAT Permissions:** The Personal Access Token used must have the appropriate permissions on Tableau Server to read workbooks, views, and view data.
* **Logging:** Check the console output and log files (if configured) for detailed error messages and operational information. The logging level can be adjusted in `config/settings.py`.
