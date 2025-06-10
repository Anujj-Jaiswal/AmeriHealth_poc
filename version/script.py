import os
import time
import pandas as pd
import smtplib
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
def send_failure_email(failed_df, file_name):
    msg = MIMEMultipart()
    msg['From'] = SENDER_EMAIL
    msg['To'] = RECIPIENT_EMAIL
    msg['Subject'] = f"[ALERT] {len(failed_df)} Failures in {file_name}"

    html_table = failed_df.to_html(index=False, border=0, justify="center", classes="data-table")

    html_body = f"""
    <html>
    <head>{html_style}</head>
    <body>
        <p>Hello,</p>
        <p>The following pipeline executions have failed in <b>{file_name}</b>:</p>
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
        print(f"Email sent for failures in {file_name}")
    except Exception as e:
        print(f"Failed to send email for {file_name} - {e}")

# === MAIN LOOP ===
print("Starting pipeline monitor...")

while True:
    print("\n--- Checking for new CSV files ---")
    
    # Load already processed files
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE, 'r') as f:
            processed_files = set(line.strip() for line in f.readlines())
    else:
        processed_files = set()

    # Process new CSV files
    for filename in os.listdir(DATA_DIR):
        if filename.endswith(".csv") and filename not in processed_files:
            csv_path = os.path.join(DATA_DIR, filename)
            try:
                df = pd.read_csv(csv_path)
                failed_runs = df[df['Status'].str.strip().str.lower() == 'failed']

                if not failed_runs.empty:
                    send_failure_email(failed_runs, filename)
                else:
                    print(f"No failures in {filename}")

                # Mark file as processed
                with open(LOG_FILE, 'a') as log:
                    log.write(f"{filename}\n")

            except Exception as e:
                print(f"Error processing {filename}: {e}")
        else:
            print(f"Skipping already processed file: {filename}")

    print("Sleeping for 5 minutes...")
    time.sleep(300)  # 5 minutes
