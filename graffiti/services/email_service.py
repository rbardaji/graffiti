import smtplib, ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from config import (smtp_server, port, sender_email, password, receiver_email,
                    admin_email)


def send_email(content: dict):
    """
    Send an email to the management email.

    Parameters
    ----------
        content: dict
            It contains the keys: 'email', 'subject' and 'message'
    
    Returns
    -------
        status_code: int
            201 - Message sent successfully
            505 - Message cannot be sent
    """
    email = content['email']
    subject = content['subject']
    message = content['message']

    # create message object instance
    msg = MIMEMultipart()
    msg['Subject'] = f'[API] {subject} - {email}'

    # add in the message body
    msg.attach(MIMEText(message, 'plain'))

    context = ssl.create_default_context()
    try:
        with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
            server.login(sender_email, password)
            server.sendmail(sender_email, receiver_email, msg.as_string())
        status_code = 201
    except smtplib.SMTPDataError:
        status_code = 505

    return status_code


def send_to_admin(content: dict):
    """
    Send an email to the management email.

    Parameters
    ----------
        content: dict
            It contains the keys: 'email', 'subject' and 'message'
    
    Returns
    -------
        status_code: int
            201 - Message sent successfully
    """
    subject = content['subject']
    message = content['message']
    # create message object instance
    msg = MIMEMultipart()
    msg['Subject'] = f'[API] {subject}'

    # add in the message body
    msg.attach(MIMEText(message, 'plain'))

    context = ssl.create_default_context()

    with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
        server.login(sender_email, password)
        server.sendmail(sender_email, admin_email, msg.as_string())

    status_code = 201

    return status_code
