import pymysql
import pandas as pd
import smtplib
import time
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# === DATABASE CONFIG ===
DB_HOST = 'localhost'
DB_PORT = 3306
DB_USER = 'root'
DB_PASSWORD = 'Hexaware@123'
DB_NAME = 'Prototype_db'
DB_TABLE = 'PipelineLogs'

# === EMAIL CONFIG ===
SENDER_EMAIL = "anuj2804j@gmail.com"
SENDER_PASSWORD = "fsue ayla orye xlht"
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
RECIPIENT_EMAIL = "anuj2804j@gmail.com"

# === TRACKING SENT RUN_IDS ===
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
def send_failure_email(failed_df):
    msg = MIMEMultipart()
    msg['From'] = SENDER_EMAIL
    msg['To'] = RECIPIENT_EMAIL
    msg['Subject'] = f"[ALERT] {len(failed_df)} New Pipeline Failures Detected"

    html_table = failed_df.to_html(index=False, border=0, justify="center", classes="data-table")

    html_body = f"""
    <html>
    <head>{html_style}</head>
    <body>
        <p>Hello,</p>
        <p>The following new pipeline executions have failed:</p>
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
        print("Email sent successfully.")
    except Exception as e:
        print(f"Failed to send email: {e}")

# === CHECK DB FUNCTION ===
def check_failed_runs():
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

        # Filter out already alerted Run_IDs
        new_failures = df[~df['Run_ID'].isin(already_alerted_run_ids)]

        if not new_failures.empty:
            send_failure_email(new_failures)
            already_alerted_run_ids.update(new_failures['Run_ID'].tolist())
        else:
            print("No new failed pipelines found.")

    except Exception as e:
        print(f"Database error: {e}")
    finally:
        if 'connection' in locals() and connection.open:
            connection.close()

# === ENTRY POINT ===
if __name__ == "__main__":
    print("Starting pipeline monitor... (press Ctrl+C to stop)")
    while True:
        check_failed_runs()
        time.sleep(300)  # 5 minutes
