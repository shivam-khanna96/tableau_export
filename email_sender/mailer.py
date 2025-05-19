# tableau_admissions_report/email_sender/mailer.py
import win32com.client as win32 # For Outlook integration
import os
import time
import datetime
import logging
from typing import List
from typing import Optional

from config import settings # For email body components

logger = logging.getLogger(__name__)

def send_outlook_email(
    recipients: List[str],
    subject: str,
    body: str,
    attachment_path: Optional[str] = None,
    display_email: bool = False
) -> bool:
    """
    Creates and sends/displays an email using the locally installed Outlook application.

    Args:
        recipients (List[str]): A list of email addresses for the 'To' field.
        subject (str): The subject line of the email.
        body (str): The HTML or plain text body of the email.
        attachment_path (Optional[str]): Absolute path to a file to attach.
        display_email (bool): If True, displays the email in Outlook instead of sending.
                              Useful for testing. Defaults to False.

    Returns:
        bool: True if the email was processed (sent/displayed) successfully, False otherwise.
    """
    if not recipients:
        logger.warning("No recipients provided for the email. Skipping email sending.")
        return False

    logger.info(f"Preparing email. To: {'; '.join(recipients)}, Subject: '{subject}'")
    if attachment_path:
        logger.info(f"Attachment path: {attachment_path}")

    try:
        logger.info("Attempting to ensure Outlook is running using os.startfile...")
        # This will try to open Outlook using the default application associated with 'outlook'
        # It might open the main Outlook window or start the application if not running.
        # This call is non-blocking.
        os.startfile("outlook") 
        logger.info("os.startfile('outlook') command issued. Pausing for 10 seconds to allow Outlook to initialize...")
        time.sleep(30) # Pause for 10 seconds
        logger.info("Pause finished. Proceeding to connect to Outlook via COM.")
    except OSError as e:
        # OSError can occur if "outlook" is not a recognized command or if there's an issue launching it.
        logger.warning(f"Could not start Outlook using os.startfile: {e}. Will proceed to try COM dispatch anyway.")
    except Exception as e_start:
        # Catch any other unexpected errors during startfile
        logger.warning(f"An unexpected error occurred while trying os.startfile('outlook'): {e_start}. Proceeding with COM dispatch.")
    # --- End of new section ---

    outlook_app = None
    try:
        # Try to get a running instance of Outlook
        logger.debug("Attempting to get active Outlook instance via COM...")
        outlook_app = win32.GetActiveObject("Outlook.Application")
        logger.debug("Successfully got active Outlook instance via COM.")
    except Exception as e:
        logger.warning(f"Could not get active Outlook instance via COM: {e}. Attempting to dispatch a new one.")
        try:
            outlook_app = win32.Dispatch("Outlook.Application")
            logger.debug("Successfully dispatched a new Outlook instance via COM.")
        except Exception as dispatch_e:
            logger.error(f"Failed to dispatch Outlook.Application via COM: {dispatch_e}", exc_info=True)
            logger.error(
                "Ensure Microsoft Outlook is installed and configured. "
                "The script attempted to start it, but COM connection failed."
            )
            return False

    if not outlook_app:
        logger.error("Failed to initialize Outlook application object via COM.")
        return False

    try:
        outlook_app = win32.Dispatch('outlook.application')
        mail_item = outlook_app.CreateItem(0)  # 0 represents olMailItem (standard email)

        mail_item.To = "; ".join(recipients) # Outlook expects a semicolon-separated string
        mail_item.Subject = subject
        # mail_item.Body = body # For plain text body
        mail_item.HTMLBody = body # For HTML body, more flexible

        if attachment_path:
            if not os.path.isabs(attachment_path):
                logger.warning(f"Attachment path '{attachment_path}' is not absolute. Attempting to resolve.")
                # Attempt to make it absolute if it's relative to project root or similar
                # This part might need adjustment based on where attachment_path is generated
                attachment_path = os.path.abspath(attachment_path)

            if os.path.exists(attachment_path):
                mail_item.Attachments.Add(Source=attachment_path)
                logger.info(f"Successfully added attachment: {attachment_path}")
            else:
                logger.error(f"Attachment file not found at: {attachment_path}. Email will be sent without it.")
                # Depending on requirements, you might choose not to send or raise an error.

        if display_email:
            mail_item.Display()
            logger.info("Email displayed in Outlook for review.")
        else:
            mail_item.Send()
            logger.info("✅ Email sent successfully via Outlook.")
        return True

    except FileNotFoundError as fnf_err: # Specifically for attachment issues
        logger.error(f"Error attaching file for email (file not found): {fnf_err}", exc_info=True)
        return False
    except Exception as e:
        # This can catch various errors, including Outlook not being open,
        # permission issues, or problems with the win32com library.
        logger.error(f"Failed to create or send/display email via Outlook: {e}", exc_info=True)
        logger.error(
            "Ensure Microsoft Outlook is installed, configured, and running. "
            "If running this script as a scheduled task or service, Outlook may not be accessible "
            "due to session isolation. Consider alternative email libraries (like smtplib) for such scenarios "
            "if Outlook automation proves unreliable."
        )
        return False

def prepare_and_send_report_email(attachment_full_path: str):
    """
    Prepares the email content (subject, body) using configured settings
    and sends the email with the generated report as an attachment.
    """
    if not settings.EMAIL_RECIPIENTS_LIST:
        logger.info("No email recipients configured in settings.EMAIL_RECIPIENTS_LIST. Skipping email.")
        return

    today_formatted = datetime.date.today().strftime("%B %d, %Y")
    email_subject = f"{settings.EMAIL_SUBJECT_PREFIX} - {today_formatted}"
    
    # Constructing a simple HTML body for better formatting potential
    email_body_html = f"""
    <html>
    <body>
        <p>{settings.EMAIL_BODY_GREETING}</p>
        <p>This report was generated on: {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>
        <br>
        <p>{settings.EMAIL_BODY_SIGNATURE.replace(os.linesep, "<br>")}</p>
    </body>
    </html>
    """

    logger.info("Attempting to send the report email...")
    success = send_outlook_email(
        recipients=settings.EMAIL_RECIPIENTS_LIST,
        subject=email_subject,
        body=email_body_html,
        attachment_path=attachment_full_path,
        display_email=False # Set to True for testing to review before sending
    )

    if success:
        logger.info("Report email processed successfully.")
    else:
        logger.error("Failed to process the report email.")

