import os
import time
import pandas as pd
import smtplib
import pymysql
import sys
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from colorama import Fore, Style, init
from tabulate import tabulate

try:
    from plyer import notification
    HAS_PLYER = True
except ImportError:
    HAS_PLYER = False

# === INIT COLOR ===
init(autoreset=True)

# === CONFIGURATION ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "Data")
LOG_FILE = os.path.join(BASE_DIR, "processed_files.log")

SENDER_EMAIL = ""
SENDER_PASSWORD = ""
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
RECIPIENT_EMAIL = "AnujJ1@hexaware.com"

# === DATABASE CONFIG ===
DB_HOST = 'localhost'
DB_PORT = 3306
DB_USER = 'root'
DB_PASSWORD = 'Hexaware@123'
DB_NAME = 'Prototype_db'
DB_TABLE = 'PipelineLogs'

# === REPORT DIR ===
REPORT_DIR = os.path.join(BASE_DIR, "report")
os.makedirs(REPORT_DIR, exist_ok=True)

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

# === GLOBAL FAILURE COLLECTION ===
collected_failures = []

def log(msg, color=Fore.WHITE):
    print(color + f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

def countdown(seconds):
    for i in range(seconds, 0, -1):
        sys.stdout.write(f"\rSleeping for {i} seconds...")
        sys.stdout.flush()
        time.sleep(1)
    print()

def notify(title, message):
    if HAS_PLYER:
        notification.notify(title=title, message=message, timeout=5)

def save_collected_failures():
    if collected_failures:
        combined_df = pd.concat(collected_failures, ignore_index=True)
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        report_path = os.path.join(REPORT_DIR, f"Failure_Report_{timestamp}.csv")
        combined_df.to_csv(report_path, index=False)
        log(f"Saved collected failures to {report_path}", Fore.GREEN)
    else:
        log("No failures to report in this run.", Fore.YELLOW)

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
        log(f"Email sent for {source or 'database'} failures.", Fore.GREEN)
        notify("Pipeline Alert", f"Failures detected in {source or 'Database'}")
    except Exception as e:
        log(f"Failed to send email: {e}", Fore.RED)

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
        log(f"Fetched {len(df)} failed rows from DB", Fore.BLUE)

        if 'Run_ID' not in df.columns:
            log("Run_ID column missing in DB result!", Fore.RED)
            return 0

        new_failures = df[~df['Run_ID'].isin(already_alerted_run_ids)]
        if not new_failures.empty:
            send_failure_email(new_failures, source="Database")
            print(tabulate(new_failures, headers='keys', tablefmt='psql'))
            already_alerted_run_ids.update(new_failures['Run_ID'].tolist())
            collected_failures.append(new_failures.copy())
        else:
            log("No new database failures.", Fore.GREEN)

        return len(new_failures)

    except Exception as e:
        log(f"Database error: {e}", Fore.RED)
        return 0

    finally:
        if 'connection' in locals() and connection.open:
            connection.close()

# === CHECK FILE FAILURES ===
def check_failed_files():
    file_failures = 0
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
                    print(tabulate(failed_runs, headers='keys', tablefmt='psql'))
                    file_failures += len(failed_runs)
                    collected_failures.append(failed_runs.copy())
                else:
                    log(f"No failures in {filename}", Fore.GREEN)

                with open(LOG_FILE, 'a') as log_file:
                    log_file.write(f"{filename}\n")

            except Exception as e:
                log(f"Error processing {filename}: {e}", Fore.RED)
        else:
            log(f"Skipping already processed file: {filename}", Fore.YELLOW)
    return file_failures

# === MAIN LOOP ===
if __name__ == "__main__":
    log("Starting unified pipeline monitor... (Press Ctrl+C to stop)", Fore.CYAN)
    try:
        while True:
            log("\n=== Checking for failures ===", Fore.CYAN)
            file_failures = check_failed_files()
            db_failures = check_failed_db_runs()
            log(f"Summary: {file_failures} file failures | {db_failures if db_failures else 0} DB failures", Fore.MAGENTA)
            countdown(60)
    except KeyboardInterrupt:
        log("\nTermination signal received. Generating failure report...", Fore.YELLOW)
        save_collected_failures()
        log("Shutdown complete. Goodbye!", Fore.CYAN)
