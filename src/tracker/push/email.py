from __future__ import annotations

import smtplib
import ssl
from email.message import EmailMessage


class EmailPusher:
    def __init__(
        self,
        *,
        host: str,
        port: int,
        user: str | None,
        password: str | None,
        email_from: str,
        email_to: list[str],
        starttls: bool = True,
        use_ssl: bool = False,
        timeout_seconds: int = 20,
    ):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.email_from = email_from
        self.email_to = email_to
        self.starttls = starttls
        self.use_ssl = use_ssl
        self.timeout_seconds = timeout_seconds

    def send(self, *, subject: str, text: str) -> None:
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = self.email_from
        msg["To"] = ", ".join(self.email_to)
        msg.set_content(text)

        context = ssl.create_default_context()
        if self.use_ssl:
            with smtplib.SMTP_SSL(
                self.host,
                self.port,
                timeout=self.timeout_seconds,
                context=context,
            ) as server:
                if self.user and self.password:
                    server.login(self.user, self.password)
                server.send_message(msg)
            return

        with smtplib.SMTP(self.host, self.port, timeout=self.timeout_seconds) as server:
            if self.starttls:
                server.starttls(context=context)
            if self.user and self.password:
                server.login(self.user, self.password)
            server.send_message(msg)
