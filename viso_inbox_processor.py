"""
viso_inbox_processor.py
-----------------------
Flask API wrapper for deployment on Render.

Exposes:
  POST /run-viso   → runs the full VISO inbox processing pipeline
  GET  /health     → wake-up / liveness check

All secrets are read from environment variables (never hardcoded):
  IMAP_SERVER              e.g. "ssl0.ovh.net"
  IMAP_PORT                e.g. "993"
  EMAIL_ACCOUNT            your login email
  EMAIL_PASSWORD           your email password
  SHEET_URL                full Google Sheets URL
  GOOGLE_CREDENTIALS_JSON  the entire contents of credentials.json as a string

Dependencies (requirements.txt):
  flask
  PyPDF2
  gspread
  google-auth

  This code is actually Rayen Slouma's code but I took part of it for the workflow 
"""

import io
import json
import os
import re
import time
import imaplib
import email
from email.header import decode_header
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
load_dotenv()

from flask import Flask, jsonify, request
from PyPDF2 import PdfReader
import gspread
from google.oauth2.service_account import Credentials


app = Flask(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_sheet():
    """Authenticate with Google and return the target worksheet."""
    raw = os.environ.get("GOOGLE_CREDENTIALS_JSON")
    if not raw:
        raise EnvironmentError("GOOGLE_CREDENTIALS_JSON env var is not set")

    creds_info = json.loads(raw)
    creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
    gc = gspread.authorize(creds)

    sheet_url = os.environ.get("SHEET_URL")
    if not sheet_url:
        raise EnvironmentError("SHEET_URL env var is not set")

    spreadsheet = gc.open_by_url(sheet_url)
    return spreadsheet.worksheet("Suivi commandes")


def _make_summary(viso_rows_updated, viso_emails_processed, unmatched_viso, errors):
    return {
        "viso_rows_updated":     viso_rows_updated,
        "viso_emails_processed": viso_emails_processed,
        "unmatched_viso":        unmatched_viso,
        "errors":                errors,
    }


# ---------------------------------------------------------------------------
# PDF extraction
# ---------------------------------------------------------------------------

def extract_from_pdf(pdf_content: bytes) -> dict | None:
    """
    Parse a VISO PDF attachment and return extracted fields.

    Returns:
        {"commande": str, "numero": str}  on success
        None                              if required fields are missing
    """
    doc = PdfReader(io.BytesIO(pdf_content))
    text = "\n".join(page.extract_text() or "" for page in doc.pages)

    numero_match  = re.search(r"Numéro.*?(CC\d+)", text, re.DOTALL)
    commande_match = re.search(r"([A-Za-z0-9-]{5,})\s+Votre commande", text)

    if not numero_match or not commande_match:
        print("[VISO] Could not extract fields from PDF.")
        print("--- PDF text ---")
        print(text)
        print("--- End ---")
        return None

    return {
        "commande": commande_match.group(1),
        "numero":   numero_match.group(1),
    }


# ---------------------------------------------------------------------------
# Core processing logic
# ---------------------------------------------------------------------------

def process_viso_inbox_emails(
    imap_server: str,
    imap_port: int,
    email_account: str,
    password: str,
    sheet,
) -> dict:
    """
    Connect to the IMAP inbox, find VISO acknowledgement emails from the
    last 2 days, extract PDF data, and update the Google Sheet.
    """
    viso_rows_updated     = 0
    viso_emails_processed = 0
    unmatched_viso: list[str] = []
    errors:         list[str] = []

    BATCH_SIZE            = 10
    SLEEP_BETWEEN_BATCHES = 10  # seconds

    print("\n--- Processing inbox VISO emails (today + yesterday) ---")

    # ------------------------------------------------------------------
    # 1. Connect to IMAP
    # ------------------------------------------------------------------
    try:
        mail = imaplib.IMAP4_SSL(imap_server, imap_port)
        mail.login(email_account, password)
    except Exception as e:
        print(f"[VISO] IMAP connection failed: {e}")
        errors.append(f"IMAP connection failed: {e}")
        return _make_summary(viso_rows_updated, viso_emails_processed, unmatched_viso, errors)

    mail.select("inbox")

    # ------------------------------------------------------------------
    # 2. Search: FROM viso@viso-cpn.com, last 2 days
    # ------------------------------------------------------------------
    today      = datetime.now(timezone.utc)
    since_date = (today - timedelta(days=1)).strftime("%d-%b-%Y")

    status, messages = mail.search(
        None,
        f'(FROM "viso@viso-cpn.com" SINCE {since_date})'
    )

    if status != "OK" or not messages[0]:
        print("[VISO] No matching VISO emails found.")
        mail.logout()
        return _make_summary(viso_rows_updated, viso_emails_processed, unmatched_viso, errors)

    email_ids = messages[0].split()
    print(f"[VISO] Found {len(email_ids)} candidate email(s).")

    # ------------------------------------------------------------------
    # 3. Process each email
    # ------------------------------------------------------------------
    processed_any = False
    batch_count   = 0

    for num in email_ids:
        status, msg_data = mail.fetch(num, "(RFC822)")
        if status != "OK":
            continue

        msg = email.message_from_bytes(msg_data[0][1])

        # Decode subject
        raw_subject, encoding = decode_header(msg.get("Subject", ""))[0]
        if isinstance(raw_subject, bytes):
            raw_subject = raw_subject.decode(encoding or "utf-8", errors="ignore")

        # Only process acknowledgement emails
        if "Accusé" not in raw_subject:
            continue

        from_   = msg.get("From")
        date_str = msg.get("Date")
        try:
            email_date = email.utils.parsedate_to_datetime(date_str)
        except Exception:
            email_date = None

        print(f"[VISO] From: {from_} | Subject: {raw_subject} | Date: {email_date}")

        # --------------------------------------------------------------
        # 4. Find and parse the PDF attachment
        # --------------------------------------------------------------
        found_pdf = False

        for part in msg.walk():
            if part.get_content_maintype() == "multipart":
                continue
            if part.get("Content-Disposition") is None:
                continue

            filename = part.get_filename()
            if not filename:
                continue

            fname_decoded, enc = decode_header(filename)[0]
            if isinstance(fname_decoded, bytes):
                fname_decoded = fname_decoded.decode(enc or "utf-8", errors="ignore")

            if not fname_decoded.lower().endswith(".pdf"):
                continue

            found_pdf = True
            pdf_bytes = part.get_payload(decode=True)

            try:
                result = extract_from_pdf(pdf_bytes)
                if result:
                    viso_emails_processed += 1
                    commande = result["commande"]
                    numero   = result["numero"]
                    print(f"[VISO] Extracted → commande={commande}, numero={numero}")

                    # --------------------------------------------------
                    # 5. Match commande in sheet column C → write to D
                    # --------------------------------------------------
                    col_c_values = sheet.col_values(3)
                    updated = False

                    for row_idx in range(len(col_c_values), 0, -1):
                        cell_value = col_c_values[row_idx - 1]

                        if commande in cell_value:
                            try:
                                sheet.update_cell(row_idx, 4, numero)
                                print(
                                    f"[VISO] SUCCESS row {row_idx}: "
                                    f"'{commande}' in '{cell_value}' → wrote '{numero}' to D{row_idx}"
                                )
                                viso_rows_updated += 1
                                updated = True
                            except Exception as e:
                                print(f"[VISO] ERROR updating D{row_idx}: {e}")
                                errors.append(f"Failed to update D{row_idx} with {numero}: {e}")
                                updated = True   # still break — avoid duplicate writes
                            break

                    if not updated:
                        print(f"[VISO] WARNING: No match for '{commande}' — '{numero}' not written.")
                        unmatched_viso.append(commande)

                    processed_any = True

                else:
                    print("[VISO] PDF parsed but required fields missing.")

            except Exception as e:
                print(f"[VISO] PDF extraction error: {e}")
                errors.append(f"VISO PDF extraction error: {e}")

            break  # only process the first PDF attachment per email

        if not found_pdf:
            print("[VISO] No PDF attachment found in this email.")

        # Mark as read
        mail.store(num, "+FLAGS", "\\Seen")

        # Batch pause to respect Google Sheets API rate limits
        batch_count += 1
        if batch_count >= BATCH_SIZE:
            print(f"[VISO] Sleeping {SLEEP_BETWEEN_BATCHES}s to avoid API rate limits...")
            time.sleep(SLEEP_BETWEEN_BATCHES)
            batch_count = 0

    if not processed_any:
        print("[VISO] No unread emails with a PDF attachment and subject 'Accusé' found.")

    mail.logout()
    return _make_summary(viso_rows_updated, viso_emails_processed, unmatched_viso, errors)


# ---------------------------------------------------------------------------
# Flask routes
# ---------------------------------------------------------------------------

@app.route("/health", methods=["GET"])
def health():
    """Liveness check — also used to wake up the Render instance."""
    return jsonify({"status": "ok"}), 200


@app.route("/run-viso", methods=["POST"])
def run_viso():
    """
    Trigger the full VISO inbox processing pipeline.
    Returns the summary dict as JSON.
    """
    # Read required env vars
    missing = [
        v for v in (
            "IMAP_SERVER", "EMAIL_ACCOUNT", "EMAIL_PASSWORD",
            "SHEET_URL", "GOOGLE_CREDENTIALS_JSON"
        )
        if not os.environ.get(v)
    ]
    if missing:
        return jsonify({"error": f"Missing env vars: {', '.join(missing)}"}), 500

    try:
        sheet = _get_sheet()
    except Exception as e:
        return jsonify({"error": f"Google Sheets auth failed: {e}"}), 500

    summary = process_viso_inbox_emails(
        imap_server=os.environ["IMAP_SERVER"],
        imap_port=int(os.environ.get("IMAP_PORT", "993")),
        email_account=os.environ["EMAIL_ACCOUNT"],
        password=os.environ["EMAIL_PASSWORD"],
        sheet=sheet,
    )

    # Return 200 even if there were soft errors — let n8n inspect the body
    return jsonify(summary), 200


# ---------------------------------------------------------------------------
# Local entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
