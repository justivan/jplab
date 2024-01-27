from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from config import Config
import smtplib
import ssl
import os


class EmailSender:
    def __init__(self, subject, to, cc=[]):
        self.server = Config.MAIL_SERVER
        self.port = Config.MAIL_PORT
        self.username = Config.MAIL_USERNAME
        self.password = Config.MAIL_PASSWORD
        self.to = to
        self.cc = cc
        self.subject = subject
        
    def send_email(self, outfile):
        # Create a multipart message and set headers
        message = MIMEMultipart("alternative")
        message["From"] = "noreply@meetingpointuae.com"
        message["To"] = ", ".join(self.to)
        message["Cc"] = ", ".join(self.cc)
        message["Subject"] = self.subject

        email_content = """
            This is an automated report.

            Please do not reply.
        """

        html_content = """
            <!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">
            <html xmlns="http://www.w3.org/1999/xhtml">
            <head>
            <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
            <meta name="viewport" content="width=device-width" />
            <title>Title</title>
            <link rel="stylesheet" href="css/foundation-emails.css" />
            </head>

            <body>
            <!-- <style> -->
            <table class="body" data-made-with-foundation>
                <tr>
                <td align="left" valign="top" style="margin: 0px; color: #17202A; text-align: left; font-family: arial; font-size: 12px;">
                    <p>This is an automated report.</p>
                    <p>Please do not reply.</p>          
                </td>
                </tr>
            </table>
            </body>
            </html>
        """
        
        message.attach(MIMEText(html_content, "html"))
        message.attach(MIMEText(email_content, "plain"))

        for f in outfile:
            with open(f, "rb") as attachment:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(attachment.read())

            encoders.encode_base64(part)
            part.add_header(
                "Content-Disposition",
                f"attachment; filename={f}",
            )

            message.attach(part)
            try:
                os.remove(f)
            except OSError as e:
                # If it fails, inform the user.
                print("Error: %s - %s." % (e.filename, e.strerror))

        # Log in to server using secure context and send email
        context = ssl.create_default_context()
        with smtplib.SMTP(self.server, self.port) as server:
            server.starttls(context=context)  # Secure the connection with TLS
            server.login(self.username, self.password)
            server.send_message(message)
