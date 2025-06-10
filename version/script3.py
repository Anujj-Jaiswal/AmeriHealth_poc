import os
import time
import pandas as pd
import smtplib
import pymysql
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# === CONFIGURATION ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "Data")
LOG_FILE = os.path.join(BASE_DIR, "processed_files.log")

SENDER_EMAIL = "anuj2804j@gmail.com"
SENDER_PASSWORD = "fsue ayla orye xlht"
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
RECIPIENT_EMAIL = "anuj2804j@gmail.com"

# === DATABASE CONFIG ===
DB_HOST = 'localhost'
DB_PORT = 3306
DB_USER = 'root'
DB_PASSWORD = 'Hexaware@123'
DB_NAME = 'Prototype_db'
DB_TABLE = 'PipelineLogs'

# === TRACKING DB RUN_IDS ===
already_alerted_run_ids = set()

# === HTML STYLE ===
html_style = """
<style>
.data-table {
    font-family: Arial, sans-serif;
    border-collapse: collapse;
    width: 100%;
}
.data-table td, .data-table th {
    border: 1px solid #dddddd;
    text-align: left;
    padding: 8px;
}
.data-table tr:nth-child(even) {
    background-color: #f9f9f9;
}
.data-table th {
    background-color: #f2a2a2;
    color: black;
}
</style>
"""

# === SEND EMAIL FUNCTION ===
def send_failure_email(failed_df, source=""):
    subject = f"[ALERT] {len(failed_df)} Failures Detected"
    if source:
        subject += f" in {source}"

    msg = MIMEMultipart()
    msg['From'] = SENDER_EMAIL
    msg['To'] = RECIPIENT_EMAIL
    msg['Subject'] = subject

    html_table = failed_df.to_html(index=False, border=0, justify="center", classes="data-table")

    html_body = f"""
    <html>
    <head>{html_style}</head>
    <body>
        <p>Hello,</p>
        <p>The following pipeline executions have failed:</p>
        {html_table}
        <p>Please review the errors and take necessary actions.</p>
        <p>Regards,<br>Pipeline Monitor Bot</p>
    </body>
    </html>
    """

    msg.attach(MIMEText(html_body, 'html'))

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SENDER_EMAIL, SENDER_PASSWORD)
            server.send_message(msg)
        print(f"Email sent for {source or 'database'} failures.")
    except Exception as e:
        print(f"Failed to send email: {e}")

# === CHECK DATABASE FAILURES ===
def check_failed_db_runs():
    global already_alerted_run_ids
    try:
        connection = pymysql.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )

        query = f"""
        SELECT * FROM {DB_TABLE}
        WHERE LOWER(TRIM(Status)) = 'failed'
        """
        df = pd.read_sql(query, connection)

        new_failures = df[~df['Run_ID'].isin(already_alerted_run_ids)]
        if not new_failures.empty:
            send_failure_email(new_failures, source="Database")
            already_alerted_run_ids.update(new_failures['Run_ID'].tolist())
        else:
            print("No new database failures.")

    except Exception as e:
        print(f"Database error: {e}")
    finally:
        if 'connection' in locals() and connection.open:
            connection.close()

# === CHECK FILE FAILURES ===
def check_failed_files():
    # Load already processed files
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE, 'r') as f:
            processed_files = set(line.strip() for line in f.readlines())
    else:
        processed_files = set()

    for filename in os.listdir(DATA_DIR):
        if filename.endswith(".csv") and filename not in processed_files:
            csv_path = os.path.join(DATA_DIR, filename)
            try:
                df = pd.read_csv(csv_path)
                failed_runs = df[df['Status'].str.strip().str.lower() == 'failed']

                if not failed_runs.empty:
                    send_failure_email(failed_runs, source=filename)
                else:
                    print(f"No failures in {filename}")

                # Mark as processed
                with open(LOG_FILE, 'a') as log:
                    log.write(f"{filename}\n")

            except Exception as e:
                print(f"Error processing {filename}: {e}")
        else:
            print(f"Skipping already processed file: {filename}")

# === MAIN LOOP ===
if __name__ == "__main__":
    print("Starting unified pipeline monitor... (Press Ctrl+C to stop)")

    while True:
        print("\n=== Checking for failures ===")
        check_failed_files()
        check_failed_db_runs()
        print("Sleeping for 1 minutes...\n")
        time.sleep(60)  # 5 minutes
