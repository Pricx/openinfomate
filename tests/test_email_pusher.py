from __future__ import annotations

from tracker.push.email import EmailPusher


class _DummySmtp:
    def __init__(self):
        self.starttls_called = 0
        self.login_called = 0
        self.send_called = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def starttls(self, context=None):  # noqa: ANN001
        self.starttls_called += 1

    def login(self, user, password):  # noqa: ANN001
        self.login_called += 1

    def send_message(self, msg):  # noqa: ANN001
        self.send_called += 1


def test_email_pusher_default_uses_starttls(monkeypatch):
    dummy = _DummySmtp()

    def fake_smtp(host, port, timeout):  # noqa: ANN001
        assert host == "smtp.example.com"
        assert port == 587
        assert timeout == 20
        return dummy

    def fake_smtps(host, port, timeout, context=None):  # noqa: ANN001
        raise AssertionError("SMTP_SSL should not be used when use_ssl=false")

    monkeypatch.setattr("tracker.push.email.smtplib.SMTP", fake_smtp)
    monkeypatch.setattr("tracker.push.email.smtplib.SMTP_SSL", fake_smtps)

    EmailPusher(
        host="smtp.example.com",
        port=587,
        user="u",
        password="p",
        email_from="from@example.com",
        email_to=["to@example.com"],
    ).send(subject="s", text="t")

    assert dummy.starttls_called == 1
    assert dummy.login_called == 1
    assert dummy.send_called == 1


def test_email_pusher_use_ssl_skips_starttls(monkeypatch):
    dummy = _DummySmtp()

    def fake_smtp(host, port, timeout):  # noqa: ANN001
        raise AssertionError("SMTP should not be used when use_ssl=true")

    def fake_smtps(host, port, timeout, context=None):  # noqa: ANN001
        assert host == "smtp.example.com"
        assert port == 465
        assert timeout == 20
        return dummy

    monkeypatch.setattr("tracker.push.email.smtplib.SMTP", fake_smtp)
    monkeypatch.setattr("tracker.push.email.smtplib.SMTP_SSL", fake_smtps)

    EmailPusher(
        host="smtp.example.com",
        port=465,
        user="u",
        password="p",
        email_from="from@example.com",
        email_to=["to@example.com"],
        use_ssl=True,
    ).send(subject="s", text="t")

    assert dummy.starttls_called == 0
    assert dummy.login_called == 1
    assert dummy.send_called == 1


def test_email_pusher_starttls_false(monkeypatch):
    dummy = _DummySmtp()

    def fake_smtp(host, port, timeout):  # noqa: ANN001
        return dummy

    monkeypatch.setattr("tracker.push.email.smtplib.SMTP", fake_smtp)

    EmailPusher(
        host="smtp.example.com",
        port=25,
        user=None,
        password=None,
        email_from="from@example.com",
        email_to=["to@example.com"],
        starttls=False,
    ).send(subject="s", text="t")

    assert dummy.starttls_called == 0
    assert dummy.login_called == 0
    assert dummy.send_called == 1

