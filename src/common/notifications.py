import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import os
from email.mime.base import MIMEBase
from email import encoders


def send_email(subject, body, to_addr, from_addr, attachment=None):
    """
    Send an email with an optional attachment.

    :param subject: The subject of the email.
    :param body: The body text of the email.
    :param to_addr: The recipient's email address.
    :param from_addr: The sender's email address.
    :param attachment: Optional path to a file to attach.
    """
    password = os.getenv('gmail_sender_password')  # Ensure this environment variable is set in your environment
    if not password:
        raise ValueError("EMAIL_PASSWORD environment variable is not set.")

    msg = MIMEMultipart()
    msg['From'] = from_addr
    msg['To'] = to_addr
    msg['Subject'] = subject

    # Attach the email body
    msg.attach(MIMEText(body, 'plain'))

    # Attach a file if provided
    if attachment:
        part = MIMEBase('application', 'octet-stream')
        with open(attachment, 'rb') as file:
            part.set_payload(file.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename={os.path.basename(attachment)}')
        msg.attach(part)

    # Connect to the SMTP server and send the email
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(from_addr, password)
    server.send_message(msg)
    server.quit()


# Example usage
if __name__ == '__main__':
    subject = 'Test Email from notifications module'
    body = 'This is a test email sent by the notifications module.'
    to_addr = 'mag1cfrogginger@gmail.com'  # Replace with the actual recipient's email address
    from_addr = 'harrywong2017@gmail.com'  # Replace with your actual email address

    send_email(subject, body, to_addr, from_addr)
    print('Test email sent successfully!')
