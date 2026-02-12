import requests
import base64
import os
import logging
import datetime
from typing import List, Optional

from config import settings

logger = logging.getLogger(__name__)

def _get_graph_access_token() -> Optional[str]:
    """
    Helper to retrieve the OAuth2 access token for Microsoft Graph
    using Client Credentials flow (Headless/Robot).
    """
    try:
        token_url = f"https://login.microsoftonline.com/{settings.GRAPH_TENANT_ID}/oauth2/v2.0/token"
        token_data = {
            'grant_type': 'client_credentials',
            'client_id': settings.GRAPH_CLIENT_ID,
            'client_secret': settings.GRAPH_CLIENT_SECRET,
            'scope': 'https://graph.microsoft.com/.default'
        }
        
        # logger.debug(f"Acquiring Graph API token for Client ID: {settings.GRAPH_CLIENT_ID}")
        response = requests.post(token_url, data=token_data)
        response.raise_for_status()
        
        token = response.json().get('access_token')
        # logger.debug("Successfully acquired Graph API Access Token.")
        return token

    except Exception as e:
        logger.error(f"CRITICAL: Failed to acquire Graph API Access Token. Error: {e}")
        if 'response' in locals() and response is not None:
             logger.error(f"Azure Response: {response.text}")
        return None

def send_email_via_graph(
    recipients: List[str],
    subject: str,
    body: str,
    attachment_path: Optional[str] = None
) -> bool:
    """
    Sends an email using the Microsoft Graph API (REST).
    This does NOT require Outlook to be installed or running.
    """
    if not recipients:
        logger.warning("No recipients provided. Skipping email.")
        return False

    # 1. Authenticate
    token = _get_graph_access_token()
    if not token:
        logger.error("Aborting email send due to missing authentication token.")
        return False

    sender_email = settings.GRAPH_SENDER_EMAIL
    endpoint = f"https://graph.microsoft.com/v1.0/users/{sender_email}/sendMail"
    
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }

    # 2. Build Recipient List
    to_recipients_payload = [{"emailAddress": {"address": email.strip()}} for email in recipients]

    # 3. Build Email Body
    message_payload = {
        "subject": subject,
        "body": {
            "contentType": "HTML",
            "content": body
        },
        "toRecipients": to_recipients_payload
    }

    # 4. Handle Attachment (if present)
    if attachment_path:
        if not os.path.exists(attachment_path):
            logger.error(f"Attachment file not found at: {attachment_path}. Sending email without attachment.")
        else:
            try:
                filename = os.path.basename(attachment_path)
                
                # Graph API requires attachments to be base64 encoded strings
                with open(attachment_path, "rb") as f:
                    file_content = f.read()
                content_b64 = base64.b64encode(file_content).decode("utf-8")
                
                # Determine content type (defaulting to Excel/generic binary)
                # You can make this dynamic if needed, but this works for .xlsx
                content_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                if filename.endswith(".csv"):
                    content_type = "text/csv"

                message_payload["attachments"] = [
                    {
                        "@odata.type": "#microsoft.graph.fileAttachment",
                        "name": filename,
                        "contentType": content_type,
                        "contentBytes": content_b64
                    }
                ]
                logger.info(f"Attached file: {filename} ({len(file_content)} bytes)")
            except Exception as e:
                logger.error(f"Failed to process attachment: {e}. Aborting email.")
                return False

    # 5. Send Request
    final_payload = {
        "message": message_payload,
        "saveToSentItems": "true"
    }

    try:
        logger.info(f"Sending email via Graph API to {len(recipients)} recipients as {sender_email}...")
        response = requests.post(endpoint, headers=headers, json=final_payload)
        
        # Graph API returns 202 Accepted on success
        if response.status_code == 202:
            logger.info("Email successfully accepted by Microsoft Graph API.")
            return True
        else:
            logger.error(f"Failed to send email. Status: {response.status_code}")
            logger.error(f"Response: {response.text}")
            return False

    except Exception as e:
        logger.error(f"Exception occurred while sending email via Graph API: {e}", exc_info=True)
        return False

def prepare_and_send_report_email(attachment_full_path: str):
    """
    Prepares the email content (subject, body) using configured settings
    and sends the email with the generated report as an attachment via Graph API.
    """
    if not settings.EMAIL_RECIPIENTS_LIST:
        logger.info("No email recipients configured in settings.EMAIL_RECIPIENTS_LIST. Skipping email.")
        return

    today_formatted = datetime.date.today().strftime("%B %d, %Y")
    email_subject = f"{settings.EMAIL_SUBJECT_PREFIX} - {today_formatted}"
    
    # Constructing HTML body
    email_body_html = f"""
    <html>
    <head>
        <style>
            body {{ font-family: Calibri, sans-serif; }}
        </style>
    </head>
    <body>
        <p>{settings.EMAIL_BODY_GREETING}</p>
        <p>This report was generated on: {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>
        <br>
        <p>{settings.EMAIL_BODY_SIGNATURE.replace(os.linesep, "<br>")}</p>
    </body>
    </html>
    """

    success = send_email_via_graph(
        recipients=settings.EMAIL_RECIPIENTS_LIST,
        subject=email_subject,
        body=email_body_html,
        attachment_path=attachment_full_path
    )

    if success:
        logger.info("Report email workflow finished successfully.")
    else:
        logger.error("Report email workflow failed.")