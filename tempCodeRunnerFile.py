import re
import os
import imaplib
import email
from email.header import decode_header
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from smtplib import SMTP
from datetime import datetime, timedelta

EMAIL_ADDRESS = "gps.vechicle@gmail.com"
EMAIL_PASSWORD = "viurxsrhmwurfryg"
EMAIL_SEND = "eeneeamdiral@gmail.com"
# --- Helper functions ---
def sanitize_filename(filename):
    return re.sub(r'[\\/*?:"<>|]', '_', filename)

def extract_date_range(filename):
    match = re.search(r'(\d{4}-\d{2}-\d{2} \d{2}_\d{2}_\d{2})_(\d{4}-\d{2}-\d{2} \d{2}_\d{2}_\d{2})', filename)
    if match:
        start = match.group(1).replace("_", ":")
        end = match.group(2).replace("_", ":")
        try:
            start_dt = datetime.strptime(start, "%Y-%m-%d %H:%M:%S")
            end_dt = datetime.strptime(end, "%Y-%m-%d %H:%M:%S")
            # Modified to accept any date range, not just 1 day
            return start_dt, end_dt
        except Exception as e:
            print(f"Date parsing error: {e}")
    return None, None

# --- Gmail Attachment Downloader ---

def save_attachments_from_gmail(save_directory):
    print(f"Connecting to Gmail...")
    mail = imaplib.IMAP4_SSL('imap.gmail.com')
    
    try:
        mail.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
    except imaplib.IMAP4.error as e:
        print(f"Login failed: {e}")
        return []
    
    mail.select('inbox')

    # Get last 3 days in IMAP format (matches the script)
    date_since = (datetime.now() - timedelta(days=3)).strftime('%d-%b-%Y')

    # Search for emails since that date
    print(f"Searching for emails since {date_since}...")
    status, email_ids = mail.search(None, f'(SINCE {date_since})')
    email_ids = email_ids[0].split()
    
    print(f"Found {len(email_ids)} emails to check.")

    if not os.path.exists(save_directory):
        os.makedirs(save_directory)

    attachment_files = []
    print(f"Downloading attachments from emails SINCE {date_since}:\n")

    for e_id in email_ids:
        try:
            status, data = mail.fetch(e_id, '(RFC822)')
            raw_email = data[0][1]
            msg = email.message_from_bytes(raw_email)

            subject = msg.get("Subject", "")
            decoded_subject = decode_header(subject)
            full_subject = ''.join(
                part.decode(enc or "utf-8", errors="ignore") if isinstance(part, bytes) else part
                for part, enc in decoded_subject
            )

            has_attachment = False

            for part in msg.walk():
                if part.get_content_maintype() == 'multipart':
                    continue
                if part.get('Content-Disposition') is None:
                    continue

                file_name = part.get_filename()
                if file_name:
                    has_attachment = True
                    safe_file_name = sanitize_filename(file_name)
                    file_path = os.path.join(save_directory, safe_file_name)
                    
                    print(f"Saving: {safe_file_name}")
                    with open(file_path, 'wb') as f:
                        f.write(part.get_payload(decode=True))
                    attachment_files.append(file_path)

            if has_attachment:
                print(f"- Downloaded from email: '{full_subject}'")
        except Exception as e:
            print(f"Error processing email: {e}")

    mail.logout()
    print(f"Total attachments downloaded: {len(attachment_files)}")
    return attachment_files

# --- Email Sender ---

def send_email_with_attachment(to_email, subject, body, file_path):
    from_email = EMAIL_ADDRESS
    password = EMAIL_PASSWORD

    print(f"Preparing to send email to {to_email}...")
    
    msg = MIMEMultipart()
    msg['From'] = from_email
    msg['To'] = to_email
    msg['Subject'] = subject
    msg.attach(MIMEText(body))

    try:
        with open(file_path, 'rb') as attachment:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(attachment.read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', f'attachment; filename={os.path.basename(file_path)}')
            msg.attach(part)

        with SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            server.login(from_email, password)
            server.send_message(msg)
            print(f"Email sent successfully to {to_email}")
    except Exception as e:
        print(f"Failed to send email: {e}")

# --- Main Script ---

if __name__ == "__main__":
    save_dir = "./gmail_attachments"
    print("Starting Gmail attachment downloader...")
    extracted_files = save_attachments_from_gmail(save_dir)
    
    if not extracted_files:
        print("No attachments were downloaded. Check email credentials and inbox content.")
        exit(1)

    # Organize by date range
    print("\nOrganizing files by date range...")
    gps_pairs = {}
    for f in extracted_files:
        base = os.path.basename(f)
        print(f"Processing: {base}")
        start, end = extract_date_range(base)
        if start and end:
            key = (start, end)
            if key not in gps_pairs:
                gps_pairs[key] = {}
            if "fuel" in base.lower():
                gps_pairs[key]['fuel'] = f
                print(f"  - Identified as fuel file")
            elif "engine" in base.lower():
                gps_pairs[key]['engine'] = f
                print(f"  - Identified as engine file")
            elif "road" in base.lower():
                gps_pairs[key]['road'] = f
                print(f"  - Identified as road file")
        else:
            print(f"  - No valid date range found in filename")

    # Filter only complete sets
    valid_pairs = [(k, v) for k, v in gps_pairs.items() 
                  if 'fuel' in v and 'engine' in v and 'road' in v]
    valid_pairs.sort(key=lambda x: x[0][0], reverse=True)

    print(f"\nFound {len(valid_pairs)} complete sets of files.")
    
    if valid_pairs:
        latest_key, latest_files = valid_pairs[0]
        start_date, end_date = latest_key
        print(f"\nAnalyzing latest dataset ({start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}):")
        
        file_path1 = latest_files['fuel']
        file_path2 = latest_files['road']
        engine_file = latest_files['engine']
        
        print(f"- Fuel file: {os.path.basename(file_path1)}")
        print(f"- Road file: {os.path.basename(file_path2)}")
        print(f"- Engine file: {os.path.basename(engine_file)}")
        
        try:
            # Import the analysis module
            try:
                from fuel_analysis import main
                print("\nRunning fuel analysis...")
                excel_file, num_datasets = main(file_path1, file_path2, engine_file)
                
                # Force the output file name
                custom_excel_name = "UAZday1.xlsx"
                if excel_file:
                    new_excel_path = os.path.join(os.path.dirname(excel_file), custom_excel_name)
                    os.rename(excel_file, new_excel_path)

                    print(f"\nAnalysis of {num_datasets} datasets exported to {new_excel_path}")
                    
                    # Send email with attachment
                    send_email_with_attachment(
                        EMAIL_SEND,
                        "Fuel Analysis Report",
                        f"Please find the attached fuel analysis report for {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}.",
                        new_excel_path
                    )
                else:
                    print("Failed to export analysis to Excel")
            except ImportError:
                print("\nERROR: Could not import fuel_analysis module.")
                print("Make sure the fuel_analysis.py file is in the same directory as this script.")
        except Exception as e:
            print(f"\nError during analysis: {e}")
    else:
        print("\nNo valid complete sets found for analysis.")
        print("Make sure your emails contain attachments with 'fuel', 'engine', and 'road' in their filenames")
        print("and that the filenames contain valid date ranges in the format: YYYY-MM-DD HH_MM_SS_YYYY-MM-DD HH_MM_SS")