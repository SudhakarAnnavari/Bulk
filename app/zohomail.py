import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from loguru import logger
import os

def send_zoho_mail(
    url,
    password,
    total,
    unprocessed,
    to_email,
    skipped,
    not_existing,
    invalid,
    invalid_s3_url,
    attachment_path,  
):
    """this will send the mail from zoho"""
    # initialize connection to our email server, we will use Outlook here
    logger.info("Sending zoho mail")
    smtp = smtplib.SMTP(
        "smtp.zoho.com", port="587"
    )  # smtp = smtplib.SMTP('smtp-mail.outlook.com', port='587') # Note: 465 crashes immediately
    smtp.ehlo()  # send the extended hello to our server - seems to work without it.
    smtp.starttls()  # tell server we want to communicate with TLS encryption
    smtp.login("support@finkraft.ai", "Support.f@1234")

    # msg = 'My Test Mail '
    msg = MIMEMultipart("alternative")
    msg["Subject"] = "Your download is ready"
    msg["From"] = "support@finkraft.ai"
    msg["To"] = to_email
    message = "<html><head></head><body><strong>Click the link to download your zipped file<br/> {} <br/> Total files: {} <br/> Unprocessed files: {} <br/> Password to unpack the zip file:: {} </strong> </br> skipped: {} </br> not_existing: {} </br> invalid: {} </br> invalid_s3_url: {} </br> </body></html>.".format(
        url,
        total,
        unprocessed,
        password,
        skipped,
        not_existing,
        invalid,
        invalid_s3_url,
    )
    msg.attach(MIMEText(message, "html"))

    # Attach the CSV file
    with open(attachment_path, "rb") as attachment:
        part = MIMEBase("application", "octet-stream")
        part.set_payload(attachment.read())
    encoders.encode_base64(part)
    part.add_header(
        "Content-Disposition",
        f"attachment; filename= {os.path.basename(attachment_path)}",
    )
    msg.attach(part)

    # send our email message 'msg' to our boss
    smtp.sendmail(
        from_addr="support@finkraft.ai", to_addrs=to_email, msg=msg.as_string()
    )
    smtp.quit()
    print("Sent")


if __name__ == "__main__":
    send_zoho_mail(
        "--URL will be here--",
        "--zip password here--",
        "--total val here--",
        "--unprocessed val here--",
        "chakrabortybinayaka.work@gmail.com",
    )
