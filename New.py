import os
import time
import pandas as pd
import smtplib
import pymysql
import sys
import json
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from colorama import Fore, init
from tabulate import tabulate

try:
    from plyer import notification
    HAS_PLYER = True
except ImportError:
    HAS_PLYER = False

init(autoreset=True)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

SENDER_EMAIL = "anuj2804j@gmail.com"
SENDER_PASSWORD = "fsue ayla orye xlht"
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
RECIPIENT_EMAIL = "AnujJ1@hexaware.com"

DB_HOST = 'localhost'
DB_PORT = 3306
DB_USER = 'root'
DB_PASSWORD = 'Hexaware@123'
DB_NAME = 'prototype2'
DB_TABLE = 'PipelineRuns'

REPORT_DIR = os.path.join(BASE_DIR, "report")
LOG_FILE = os.path.join(BASE_DIR, "pipeline_status.json")
os.makedirs(REPORT_DIR, exist_ok=True)

pipeline_status = {}
last_checked_time = None

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

def load_pipeline_log():
    global pipeline_status
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE, 'r') as f:
            pipeline_status = json.load(f)
            for k in pipeline_status:
                pipeline_status[k]['last_run'] = pd.to_datetime(pipeline_status[k]['last_run'])
                if 'reminder' not in pipeline_status[k]:
                    pipeline_status[k]['reminder'] = 0
    else:
        pipeline_status = {}

def save_pipeline_log():
    serializable_status = {
        k: {
            **v,
            "last_run": v['last_run'].strftime('%Y-%m-%d %H:%M:%S')
        } for k, v in pipeline_status.items()
    }
    with open(LOG_FILE, 'w') as f:
        json.dump(serializable_status, f, indent=4)

def save_collected_failures():
    unresolved = [
        {
            "PipelineName": k,
            "RunStart": v["last_run"],
            "Status": v["status"],
            "Error": v["error"],
            "RunID": v["runid"]
        }
        for k, v in pipeline_status.items() if v["status"].lower() == "failed"
    ]
    if unresolved:
        df = pd.DataFrame(unresolved)
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        report_path = os.path.join(REPORT_DIR, f"Failure_Report_{timestamp}.csv")
        df.to_csv(report_path, index=False)
        log(f"Saved unresolved failures to {report_path}", Fore.GREEN)
    else:
        log("No unresolved failures to report.", Fore.YELLOW)

html_style = """<style>.data-table {font-family: Arial, sans-serif;border-collapse: collapse;width: 100%;} .data-table td, .data-table th {border: 1px solid #dddddd;text-align: left;padding: 8px;max-width: 300px;word-wrap: break-word;} .data-table tr:nth-child(even) {background-color: #f9f9f9;} .data-table th {background-color: #f2a2a2;color: black;}</style>"""

def send_failure_email(new_failures_df, resolved_pipelines, reminder_level=None):
    if reminder_level == 1:
        subject = f"[REMINDER] 6+ Hour Unresolved Failures"
    elif reminder_level == 2:
        subject = f"[URGENT REMINDER] 24+ Hour Unresolved Failures"
    else:
        subject = f"[ALERT] {len(new_failures_df)} Failures Detected"

    msg = MIMEMultipart()
    msg['From'] = SENDER_EMAIL
    msg['To'] = RECIPIENT_EMAIL
    msg['Subject'] = subject

    if not new_failures_df.empty:
        display_df = new_failures_df.copy()
        if "Error" in display_df.columns:
            display_df["Error"] = display_df["Error"].apply(
                lambda err: f'<span title="{err}">{err[:50]}{"..." if len(err) > 50 else ""}</span>' if pd.notna(err) else ""
            )
        unresolved_table = display_df.to_html(index=False, border=0, escape=False, justify="center", classes="data-table")
    else:
        unresolved_table = "<p>No unresolved failures.</p>"

    if resolved_pipelines:
        resolved_data = [
            {
                "PipelineName": name,
                "LastFailed": v["last_run"].strftime('%Y-%m-%d %H:%M:%S'),
                "Error": v["error"],
                "RunID": v["runid"]
            } for name, v in resolved_pipelines.items()
        ]
        resolved_df = pd.DataFrame(resolved_data)
        resolved_df["Error"] = resolved_df["Error"].apply(
            lambda err: f'<span title="{err}">{err[:50]}{"..." if len(err) > 50 else ""}</span>' if pd.notna(err) else ""
        )
        resolved_table = resolved_df.to_html(index=False, border=0, escape=False, justify="center", classes="data-table")
    else:
        resolved_table = "<p>No pipelines were resolved in this session.</p>"

    html_body = f"""
    <html>
    <head>{html_style}</head>
    <body>
        <p>Hello,</p>
        <p><b>Failures:</b></p>
        {unresolved_table}
        <br/>
        <p><b>Previously Failed Pipelines That Got Resolved:</b></p>
        {resolved_table}
        <p style="margin-top:20px;">Regards,<br>Pipeline Monitor Bot</p>
    </body>
    </html>
    """

    msg.attach(MIMEText(html_body, 'html'))
    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SENDER_EMAIL, SENDER_PASSWORD)
            server.send_message(msg)
        log("Email sent.", Fore.GREEN)
        notify("Pipeline Alert", "Email sent")
    except Exception as e:
        log(f"Email failed: {e}", Fore.RED)

def check_new_runs():
    global last_checked_time
    try:
        connection = pymysql.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        query = f"SELECT * FROM {DB_TABLE}"
        if last_checked_time:
            query += f" WHERE RunStart > '{last_checked_time.strftime('%Y-%m-%d %H:%M:%S')}'"
        df = pd.read_sql(query, connection)
        if not df.empty:
            df['RunStart'] = pd.to_datetime(df['RunStart'])
            last_checked_time = df['RunStart'].max()
        return df
    except Exception as e:
        log(f"Database error: {e}", Fore.RED)
        return pd.DataFrame()
    finally:
        if 'connection' in locals() and connection.open:
            connection.close()

def process_runs(df):
    new_failures = []
    resolved_pipelines = {}

    for _, row in df.iterrows():
        name = row['PipelineName']
        status = row['Status'].strip().lower()
        run_time = row['RunStart']
        error = row['Error']
        runid = row['RunID']

        prev = pipeline_status.get(name)
        if status == "failed":
            pipeline_status[name] = {
                "status": "Failed",
                "last_run": run_time,
                "error": error,
                "runid": runid,
                "reminder": 0
            }
            if not prev or prev["status"].lower() != "failed":
                new_failures.append(row)
        elif status == "succeeded":
            if prev and prev["status"].lower() == "failed":
                resolved_pipelines[name] = prev
            pipeline_status[name] = {
                "status": "Succeeded",
                "last_run": run_time,
                "error": "",
                "runid": runid,
                "reminder": 0
            }

    if new_failures or resolved_pipelines:
        send_failure_email(pd.DataFrame(new_failures), resolved_pipelines)

def check_reminders_and_send():
    now = pd.Timestamp.now()
    reminders_to_send = []

    for name, info in pipeline_status.items():
        if info["status"].lower() == "failed":
            hours_passed = (now - info["last_run"]).total_seconds() / 3600
            reminder_level = info.get("reminder", 0)
            if 6 <= hours_passed < 24 and reminder_level < 1:
                reminders_to_send.append((name, info, 1))
            elif hours_passed >= 24 and reminder_level < 2:
                reminders_to_send.append((name, info, 2))

    if reminders_to_send:
        df_data = [
            {
                "PipelineName": name,
                "RunStart": info["last_run"],
                "Status": "Failed",
                "Error": info["error"],
                "RunID": info["runid"]
            }
            for name, info, _ in reminders_to_send
        ]
        reminder_df = pd.DataFrame(df_data)
        max_level = max(lvl for _, _, lvl in reminders_to_send)
        send_failure_email(reminder_df, resolved_pipelines={}, reminder_level=max_level)

        for name, _, lvl in reminders_to_send:
            pipeline_status[name]["reminder"] = lvl

if __name__ == "__main__":
    load_pipeline_log()
    log("Starting intelligent pipeline monitor...", Fore.CYAN)
    try:
        while True:
            log("\nChecking for new database runs...", Fore.CYAN)
            df = check_new_runs()
            if not df.empty:
                log(f"{len(df)} new rows fetched.", Fore.BLUE)
                process_runs(df)
            else:
                log("No new runs.", Fore.GREEN)
            check_reminders_and_send()
            save_pipeline_log()
            countdown(60)
    except KeyboardInterrupt:
        log("\nTermination detected. Generating final report...", Fore.YELLOW)
        save_collected_failures()
        log("Shutdown complete.", Fore.CYAN)
