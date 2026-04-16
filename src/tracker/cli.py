from __future__ import annotations

import asyncio
import datetime as dt
import json
import sys
from pathlib import Path

import typer
from rich.console import Console

from tracker.db import create_engine_from_settings, session_factory
from tracker.logging_config import configure_logging
from tracker.models import Base
from tracker.repo import Repo
from tracker.settings import get_settings
from tracker.actions import (
    TopicSpec,
    TopicAiPolicySpec,
    SourceBindingSpec,
    accept_source_candidate as accept_source_candidate_action,
    create_binding as create_binding_action,
    create_discourse_source as create_discourse_source_action,
    create_html_list_source as create_html_list_source_action,
    create_hn_search_source as create_hn_search_source_action,
    create_llm_models_source as create_llm_models_source_action,
    create_rss_source as create_rss_source_action,
    create_rss_sources_bulk as create_rss_sources_bulk_action,
    create_searxng_search_source as create_searxng_search_source_action,
    create_topic as create_topic_action,
    ignore_source_candidate as ignore_source_candidate_action,
    remove_binding as remove_binding_action,
    set_topic_enabled as set_topic_enabled_action,
    sync_topic_search_sources as sync_topic_search_sources_action,
    upsert_topic_ai_policy as upsert_topic_ai_policy_action,
    update_source_meta as update_source_meta_action,
    update_binding as update_binding_action,
)

app = typer.Typer(no_args_is_help=True, add_completion=False)
console = Console()


@app.callback()
def _main():
    settings = get_settings()
    configure_logging(level=settings.log_level)


@app.command("version")
def version():
    from tracker import __version__

    console.print(__version__)


@app.command("stats")
def stats():
    from sqlalchemy.exc import OperationalError

    settings = get_settings()
    _engine, make_session = session_factory(settings)
    with make_session() as session:
        try:
            s = Repo(session).get_stats()
        except OperationalError as exc:
            msg = str(getattr(exc, "orig", exc))
            if "no such table" in msg or "doesn't exist" in msg:
                console.print("[red]ERROR[/red] DB is not initialized (or TRACKER_DB_URL points to the wrong DB).")
                console.print("Hint: run `tracker db init` (and ensure you run from the project dir, or set TRACKER_DB_URL).")
                raise typer.Exit(1) from exc
            raise
    for k in sorted(s.keys()):
        console.print(f"- {k}: {s[k]}")


@app.command("doctor")
def doctor():
    """
    Print a quick diagnostic report for operators.
    """
    from sqlalchemy.exc import OperationalError

    from tracker.doctor import build_doctor_report

    settings = get_settings()
    _engine, make_session = session_factory(settings)

    db_ok = True
    db_error: str | None = None
    stats: dict[str, int] = {}
    profile_configured = False
    telegram_chat_configured = False

    with make_session() as session:
        try:
            repo = Repo(session)
            stats = repo.get_stats()
            profile_configured = bool(repo.get_app_config("profile_text"))
            telegram_chat_configured = bool(repo.get_app_config("telegram_chat_id"))
            activity = repo.get_activity_snapshot()
        except OperationalError as exc:
            db_ok = False
            msg = str(getattr(exc, "orig", exc))
            db_error = msg
            if "no such table" in msg or "doesn't exist" in msg:
                console.print("[red]ERROR[/red] DB is not initialized (or TRACKER_DB_URL points to the wrong DB).")
                console.print("Hint: run `tracker db init` (and ensure you run from the project dir, or set TRACKER_DB_URL).")
                raise typer.Exit(1) from exc
            raise

    report = build_doctor_report(
        settings=settings,
        stats=stats,
        db_ok=db_ok,
        db_error=db_error,
        profile_configured=profile_configured,
        telegram_chat_configured=telegram_chat_configured,
        activity=activity if db_ok else None,
    )

    console.print("# OpenInfoMate Doctor")
    console.print(f"- db_url: {settings.db_url}")
    console.print(
        f"- cron_timezone: {report.cron_timezone} ok={str(report.cron_timezone_ok).lower()} now={report.cron_now_iso}"
    )
    if report.next_health_report_at or report.next_discover_sources_at:
        console.print(
            "- next: "
            f"health_at={report.next_health_report_at or 'none'} "
            f"discover_sources_at={report.next_discover_sources_at or 'none'}"
        )
    console.print(
        "- push: "
        f"dingtalk={str(report.push_dingtalk_configured).lower()} "
        f"telegram={str(report.push_telegram_configured).lower()} "
        f"email={str(report.push_email_configured).lower()} "
        f"webhook={str(report.push_webhook_configured).lower()}"
    )
    console.print(f"- profile: configured={str(report.profile_configured).lower()}")
    console.print(
        "- activity: "
        f"last_tick_at={report.last_tick_at or 'none'} "
        f"last_digest_report_at={report.last_digest_report_at or 'none'} "
        f"last_health_report_at={report.last_health_report_at or 'none'} "
        f"last_push_sent_at={report.last_push_sent_at or 'none'}"
    )
    console.print(
        "- scheduler: "
        f"last_digest_sync_at={report.last_digest_sync_at or 'none'} "
        f"last_curated_sync_at={report.last_curated_sync_at or 'none'}"
    )
    if report.push_missing_env:
        console.print("- push_env_missing:")
        for ch in ["dingtalk", "telegram", "email", "webhook"]:
            req = report.push_missing_env.get(ch)
            if req:
                console.print(f"  - {ch}: {', '.join(req)}")

    console.print("- stats:")
    for k in sorted(report.stats.keys()):
        console.print(f"  - {k}: {report.stats[k]}")

    if report.recommendations:
        console.print("- recommendations:")
        for r in report.recommendations:
            console.print(f"  - {r}")


db_app = typer.Typer(no_args_is_help=True)
topic_app = typer.Typer(no_args_is_help=True)
topic_policy_app = typer.Typer(no_args_is_help=True)
source_app = typer.Typer(no_args_is_help=True)
bind_app = typer.Typer(no_args_is_help=True)
run_app = typer.Typer(no_args_is_help=True)
service_app = typer.Typer(no_args_is_help=True)
api_app = typer.Typer(no_args_is_help=True)
config_app = typer.Typer(no_args_is_help=True)
env_app = typer.Typer(no_args_is_help=True)
push_app = typer.Typer(no_args_is_help=True)
candidate_app = typer.Typer(no_args_is_help=True)
event_app = typer.Typer(no_args_is_help=True)
report_app = typer.Typer(no_args_is_help=True)
llm_app = typer.Typer(no_args_is_help=True)
profile_app = typer.Typer(no_args_is_help=True)
app.add_typer(db_app, name="db")
app.add_typer(topic_app, name="topic")
topic_app.add_typer(topic_policy_app, name="policy")
app.add_typer(source_app, name="source")
app.add_typer(bind_app, name="bind")
app.add_typer(run_app, name="run")
app.add_typer(service_app, name="service")
app.add_typer(api_app, name="api")
app.add_typer(config_app, name="config")
app.add_typer(env_app, name="env")
app.add_typer(push_app, name="push")
app.add_typer(candidate_app, name="candidate")
app.add_typer(event_app, name="event")
app.add_typer(report_app, name="report")
app.add_typer(llm_app, name="llm")
app.add_typer(profile_app, name="profile")


@llm_app.command("usage")
def llm_usage(
    hours: int = typer.Option(1, "--hours", help="Look back window (UTC)."),
    as_json: bool = typer.Option(False, "--json", help="Output JSON to stdout."),
):
    """
    Show recorded LLM token usage (and estimated USD cost when pricing is configured).
    """
    from sqlalchemy.exc import OperationalError

    from tracker.llm_usage import estimate_llm_cost_usd

    if hours <= 0:
        raise typer.BadParameter("--hours must be > 0")

    settings = get_settings()
    _engine, make_session = session_factory(settings)

    since = dt.datetime.utcnow() - dt.timedelta(hours=int(hours))
    try:
        with make_session() as session:
            summary = Repo(session).summarize_llm_usage(since=since)
    except OperationalError as exc:
        msg = str(getattr(exc, "orig", exc))
        if "no such table" in msg or "doesn't exist" in msg:
            console.print("[red]ERROR[/red] DB is missing llm_usage table.")
            console.print("Hint: run `tracker db init` (or redeploy on the server).")
            raise typer.Exit(1) from exc
        raise

    cost = estimate_llm_cost_usd(
        prompt_tokens=int(summary.get("prompt_tokens") or 0),
        completion_tokens=int(summary.get("completion_tokens") or 0),
        input_per_million_usd=float(settings.llm_price_input_per_million_usd or 0.0),
        output_per_million_usd=float(settings.llm_price_output_per_million_usd or 0.0),
    )

    result = {
        "hours": int(hours),
        "since": since.isoformat() + "Z",
        "pricing": {
            "input_per_million_usd": float(settings.llm_price_input_per_million_usd or 0.0),
            "output_per_million_usd": float(settings.llm_price_output_per_million_usd or 0.0),
        },
        "summary": summary,
        "estimated_usd": cost,
    }

    if as_json:
        console.print(json.dumps(result, ensure_ascii=False, indent=2))
        return

    console.print(f"# LLM Usage (last {hours}h)")
    console.print(f"- calls: {summary.get('calls', 0)}")
    console.print(f"- prompt_tokens: {summary.get('prompt_tokens', 0)}")
    console.print(f"- completion_tokens: {summary.get('completion_tokens', 0)}")
    console.print(f"- total_tokens: {summary.get('total_tokens', 0)}")
    if cost is None:
        console.print("- estimated_usd: unknown (set TRACKER_LLM_PRICE_INPUT_PER_MILLION_USD/OUTPUT...)")
    else:
        console.print(f"- estimated_usd: ${cost:.4f}")

    if int(summary.get("calls") or 0) > 0 and int(summary.get("total_tokens") or 0) == 0:
        console.print("[yellow]NOTE[/yellow] backend responses did not include token usage; totals may be zero.")


@env_app.command("set")
def env_set(
    key: str = typer.Argument(..., help="Full env var name, e.g. TRACKER_DINGTALK_WEBHOOK_URL."),
    value: str = typer.Argument(..., help="Value to write (will be stored in .env)."),
):
    """
    Set a single env var in the configured `.env` file (for operators).

    This is useful on servers where you prefer a single command instead of opening an editor.
    """
    from tracker.envfile import upsert_env_vars

    settings = get_settings()
    env_path = Path(settings.env_path or ".env")
    upsert_env_vars(path=env_path, updates={key: value})
    console.print(f"[green]OK[/green] wrote {key} to {env_path}")


@env_app.command("import")
def env_import(
    in_path: Path | None = typer.Option(None, "--in", help="Read KEY=VALUE lines from a file. Default: stdin."),
    apply: bool = typer.Option(False, "--apply", help="Apply changes (default is dry-run)."),
):
    """
    Import multiple KEY=VALUE lines into the configured `.env` file.
    """
    from tracker.envfile import parse_env_assignments, upsert_env_vars

    settings = get_settings()
    env_path = Path(settings.env_path or ".env")

    if in_path is None:
        text = sys.stdin.read()
    else:
        text = in_path.read_text(encoding="utf-8")

    updates = parse_env_assignments(text)
    if not updates:
        console.print("(no KEY=VALUE assignments found)")
        raise typer.Exit(2)

    if not apply:
        for k in sorted(updates.keys()):
            console.print(f"- {k}")
        console.print("Run with `--apply` to write these keys into the env file.")
        raise typer.Exit(0)

    upsert_env_vars(path=env_path, updates=updates)
    console.print(f"[green]OK[/green] wrote {len(updates)} keys to {env_path}")


@profile_app.command("apply")
def profile_apply(
    in_path: Path | None = typer.Option(None, "--in", help="Read profile text from a file. Default: stdin."),
    name: str = typer.Option("Profile", "--name", help="Profile topic name."),
    digest_cron: str = typer.Option("0 9 * * *", "--digest-cron", help="Digest cron for the profile topic."),
    add_hn_rss: bool = typer.Option(True, "--hn-rss/--no-hn-rss", help="Include HN frontpage RSS."),
    add_hn_popularity: bool = typer.Option(
        True,
        "--hn-popularity/--no-hn-popularity",
        help="Include HN Popularity (Karpathy 90+ blogs) RSS pack.",
    ),
    add_github_trending_daily: bool = typer.Option(
        True, "--github-daily/--no-github-daily", help="Include GitHub Trending (daily) stream."
    ),
    add_github_trending_weekly: bool = typer.Option(
        False, "--github-weekly/--no-github-weekly", help="Include GitHub Trending (weekly) stream."
    ),
    add_github_trending_monthly: bool = typer.Option(
        False, "--github-monthly/--no-github-monthly", help="Include GitHub Trending (monthly) stream."
    ),
    add_arxiv: bool = typer.Option(True, "--arxiv/--no-arxiv", help="Include arXiv RSS streams."),
    add_discourse: bool = typer.Option(False, "--discourse/--no-discourse", help="Include Discourse latest stream."),
    discourse_base_url: str = typer.Option("", "--discourse-base-url", help="Discourse base URL (e.g. https://forum.example.com)."),
    add_nodeseek: bool = typer.Option(False, "--nodeseek/--no-nodeseek", help="Include NodeSeek RSS stream."),
    github_languages: str = typer.Option("", "--github-languages", help="Optional GitHub language list (CSV)."),
    arxiv_categories: str = typer.Option(
        "cs.AI,cs.LG,cs.CL,cs.CV,stat.ML",
        "--arxiv-categories",
        help="arXiv categories (CSV).",
    ),
    add_searxng: bool = typer.Option(
        True,
        "--searxng/--no-searxng",
        help="Include SearxNG search sources using AI retrieval queries (recall only; LLM filters).",
    ),
    searxng_base_url: str = typer.Option(
        "http://127.0.0.1:8888", "--searxng-base-url", help="SearxNG base URL (e.g. http://127.0.0.1:8888)."
    ),
    run: bool = typer.Option(True, "--run/--no-run", help="Run a one-off tick+discover+digest after applying."),
    push: bool = typer.Option(False, "--push", help="Push profile digest (and a one-time profile summary) if configured."),
    hours: int = typer.Option(24, "--hours", help="Digest window hours for the kickoff run."),
):
    """
    Apply a single AI-native interest Profile from arbitrary text (e.g. bookmarks export).

    This generates:
    - a short understanding summary
    - interest axes + retrieval hints (NOT used for keyword matching)
    - a strict curation prompt (LLM reads content and decides ignore|digest|alert)
    """
    from tracker.llm import llm_propose_profile_setup
    from tracker.llm_usage import make_llm_usage_recorder
    from tracker.profile_input import normalize_profile_text
    from tracker.runner import run_digest, run_discover_sources, run_tick
    from tracker.push_dispatch import push_dingtalk_markdown, push_telegram_text, push_webhook_json

    settings = get_settings()
    if not (
        settings.llm_curation_enabled
        and settings.llm_base_url
        and (getattr(settings, "llm_model_reasoning", None) or getattr(settings, "llm_model", None))
    ):
        console.print(
            "[red]ERROR[/red] profile requires TRACKER_LLM_CURATION_ENABLED=true + configured LLM (TRACKER_LLM_BASE_URL + TRACKER_LLM_MODEL_REASONING or TRACKER_LLM_MODEL)."
        )
        raise typer.Exit(2)

    raw = sys.stdin.read() if in_path is None else in_path.read_text(encoding="utf-8")
    profile_text = normalize_profile_text(text=raw)
    if not profile_text:
        console.print("[red]ERROR[/red] empty profile text")
        raise typer.Exit(2)

    import hashlib

    profile_sig = hashlib.sha256(profile_text.encode("utf-8")).hexdigest()[:8]

    _engine, make_session = session_factory(settings)
    with make_session() as session:
        repo = Repo(session)
        usage_cb = make_llm_usage_recorder(session=session)

        proposal = asyncio.run(
            llm_propose_profile_setup(settings=settings, profile_text=profile_text, usage_cb=usage_cb)
        )
        if proposal is None:
            console.print("[red]ERROR[/red] LLM is not configured")
            raise typer.Exit(2)

        console.print("# Profile Proposal")
        console.print(f"- understanding: {proposal.understanding}", markup=False)
        if proposal.interest_axes:
            console.print("- interest_axes:")
            for a in proposal.interest_axes:
                console.print(f"  - {a}", markup=False)
        if proposal.interest_keywords:
            console.print("- interest_keywords:")
            console.print("  - " + ", ".join(proposal.interest_keywords), markup=False)
        if proposal.retrieval_queries:
            console.print("- retrieval_queries:")
            for q in proposal.retrieval_queries:
                console.print(f"  - {q}", markup=False)

        topic_name = (name or "").strip() or "Profile"
        prompt = (proposal.ai_prompt or "").strip()
        if not prompt:
            console.print("[red]ERROR[/red] missing ai_prompt from LLM")
            raise typer.Exit(2)

        # Create or update the profile topic. Query is intentionally blank (no keyword matching).
        topic = repo.get_topic_by_name(topic_name)
        if not topic:
            try:
                topic = create_topic_action(
                    session=session,
                    spec=TopicSpec(
                        name=topic_name,
                        query="",
                        digest_cron=(digest_cron or "0 9 * * *").strip() or "0 9 * * *",
                        alert_keywords="",
                    ),
                )
            except ValueError as exc:
                console.print(f"[red]ERROR[/red] {exc}", markup=False)
                raise typer.Exit(2) from exc
        else:
            topic.query = ""
            topic.digest_cron = (digest_cron or "0 9 * * *").strip() or "0 9 * * *"
            session.commit()

        # Persist profile config (single profile).
        repo.set_app_config("profile_topic_name", topic_name)
        repo.set_app_config("profile_text", profile_text)
        repo.set_app_config("profile_understanding", proposal.understanding)
        repo.set_app_config("profile_interest_axes", "\n".join(proposal.interest_axes or []))
        repo.set_app_config("profile_interest_keywords", ", ".join(proposal.interest_keywords or []))
        repo.set_app_config("profile_retrieval_queries", "\n".join(proposal.retrieval_queries or []))

        # Seed stream sources (broad recall; AI filters hard).
        if add_hn_rss:
            create_rss_source_action(
                session=session,
                url="https://news.ycombinator.com/rss",
                bind=SourceBindingSpec(topic=topic_name, include_keywords=""),
            )

        if add_hn_popularity:
            from tracker.source_packs import get_rss_pack

            pack = get_rss_pack("hn_popularity_karpathy")
            create_rss_sources_bulk_action(
                session=session,
                urls=pack.urls,
                bind=SourceBindingSpec(topic=topic_name, include_keywords=""),
                tags="hn-popularity,karpathy",
            )

        def _parse_csv_list(value: str, *, max_items: int) -> list[str]:
            raw_val = (value or "").strip()
            if not raw_val:
                return []
            s = raw_val.replace("，", ",").replace("；", ",").replace(";", ",").replace("\n", ",")
            parts = [p.strip() for p in s.split(",") if p.strip()]
            out: list[str] = []
            seen: set[str] = set()
            for p in parts:
                k = p.lower()
                if k in seen:
                    continue
                seen.add(k)
                out.append(p)
                if len(out) >= max_items:
                    break
            return out

        if add_github_trending_daily or add_github_trending_weekly or add_github_trending_monthly:
            langs = _parse_csv_list(github_languages, max_items=6)

            def _add_trending(*, since: str, language: str | None = None):
                if language:
                    from urllib.parse import quote

                    page_url = f"https://github.com/trending/{quote(language)}?since={since}"
                else:
                    page_url = f"https://github.com/trending?since={since}"
                create_html_list_source_action(
                    session=session,
                    page_url=page_url,
                    item_selector="article.Box-row",
                    title_selector="h2 a",
                    summary_selector="p",
                    max_items=25,
                    bind=SourceBindingSpec(topic=topic_name, include_keywords=""),
                )

            if add_github_trending_daily:
                _add_trending(since="daily", language=None)
                for lang in langs:
                    _add_trending(since="daily", language=lang)
            if add_github_trending_weekly:
                _add_trending(since="weekly", language=None)
                for lang in langs:
                    _add_trending(since="weekly", language=lang)
            if add_github_trending_monthly:
                _add_trending(since="monthly", language=None)
                for lang in langs:
                    _add_trending(since="monthly", language=lang)

        if add_arxiv:
            cats = _parse_csv_list(arxiv_categories, max_items=10)
            if not cats:
                cats = ["cs.AI", "cs.LG", "cs.CL", "cs.CV", "stat.ML"]
            for cat in cats:
                c = (cat or "").strip()
                if not c:
                    continue
                create_rss_source_action(
                    session=session,
                    url=f"https://export.arxiv.org/rss/{c}",
                    bind=SourceBindingSpec(topic=topic_name, include_keywords=""),
                )

        if add_discourse:
            base_url = (discourse_base_url or "").strip()
            if not base_url:
                console.print("[red]ERROR[/red] missing --discourse-base-url", markup=False)
                raise typer.Exit(2)
            create_discourse_source_action(
                session=session,
                base_url=base_url,
                json_path="/latest.json",
                bind=SourceBindingSpec(topic=topic_name, include_keywords=""),
            )

        if add_nodeseek:
            create_rss_source_action(
                session=session,
                url="https://rss.nodeseek.com/",
                bind=SourceBindingSpec(topic=topic_name, include_keywords=""),
            )

        if add_searxng and proposal.retrieval_queries:
            base = (searxng_base_url or "").strip() or "http://127.0.0.1:8888"
            for q in list(proposal.retrieval_queries or [])[:6]:
                qq = (q or "").strip()
                if not qq:
                    continue
                create_searxng_search_source_action(
                    session=session,
                    base_url=base,
                    query=qq,
                    time_range="day",
                    results=20,
                    bind=SourceBindingSpec(topic=topic_name, include_keywords=""),
                )

        upsert_topic_ai_policy_action(
            session=session,
            spec=TopicAiPolicySpec(
                topic=topic_name,
                enabled=True,
                prompt=prompt,
            ),
        )

        # Optional: one-time push of the profile summary (DingTalk only).
        async def _push_profile_summary() -> None:
            if not push:
                return
            key = f"profile_setup:{dt.datetime.utcnow().date().isoformat()}:{profile_sig}"
            lines = ["# Profile Updated", "", f"- understanding: {proposal.understanding}"]
            if proposal.interest_axes:
                lines.append("")
                lines.append("## Interest Axes")
                for a in proposal.interest_axes[:10]:
                    lines.append(f"- {a}")
            if proposal.interest_keywords:
                lines.append("")
                lines.append("## Recall Hints")
                lines.append("- " + "、".join(proposal.interest_keywords[:30]))
            if proposal.retrieval_queries:
                lines.append("")
                lines.append("## Retrieval Queries")
                for q in proposal.retrieval_queries[:8]:
                    lines.append(f"- {q}")
            md = "\n".join(lines).strip() + "\n"
            try:
                await push_dingtalk_markdown(
                    repo=repo,
                    settings=settings,
                    idempotency_key=key,
                    title="Profile Updated",
                    markdown=md,
                )
            except Exception:
                pass
            try:
                await push_telegram_text(
                    repo=repo,
                    settings=settings,
                    idempotency_key=key,
                    text=md,
                    disable_preview=True,
                )
            except Exception:
                pass
            try:
                await push_webhook_json(
                    repo=repo,
                    settings=settings,
                    idempotency_key=key,
                    payload={
                        "type": "profile_updated",
                        "topic": topic_name,
                        "understanding": proposal.understanding,
                        "interest_axes": list(proposal.interest_axes or [])[:10],
                        "interest_keywords": list(proposal.interest_keywords or [])[:40],
                        "retrieval_queries": list(proposal.retrieval_queries or [])[:12],
                    },
                )
            except Exception:
                pass

        async def _kickoff() -> None:
            from tracker.job_lock import job_lock_async

            await _push_profile_summary()
            if not run:
                return
            try:
                async with job_lock_async(name="jobs", timeout_seconds=120):
                    await run_tick(session=session, settings=settings, push=False)
                    await run_discover_sources(session=session, settings=settings, topic_ids=[int(topic.id)])
                    suffix = f"profile-{profile_sig}" if push else None
                    await run_digest(
                        session=session,
                        settings=settings,
                        hours=max(1, min(168, int(hours or 24))),
                        push=bool(push),
                        key_suffix=suffix,
                        topic_ids=[int(topic.id)],
                    )
            except TimeoutError:
                console.print("[red]ERROR[/red] busy: another job is running (try again soon)")
                return

        asyncio.run(_kickoff())
        console.print(f"[green]OK[/green] profile applied: {topic_name}")


@event_app.command("list")
def event_list(
    topic: str | None = typer.Option(None, "--topic", help="Filter by topic name."),
    decision: str | None = typer.Option(None, "--decision", help="Filter by decision: ignore|digest|alert."),
    hours: int = typer.Option(24, "--hours", help="Look back window (UTC). Use 0 to disable time filter."),
    limit: int = typer.Option(50, "--limit", help="Max rows to return."),
    as_json: bool = typer.Option(False, "--json", help="Output JSON to stdout."),
):
    """
    List recent per-topic decisions (digest/alert/ignore) with item + source info.
    """
    from sqlalchemy.exc import OperationalError

    settings = get_settings()
    _engine, make_session = session_factory(settings)

    valid_decisions = {"ignore", "digest", "alert"}
    if decision and decision not in valid_decisions:
        raise typer.BadParameter(f"invalid --decision: {decision} (expected one of: {', '.join(sorted(valid_decisions))})")

    since = None
    if hours and hours > 0:
        since = dt.datetime.utcnow() - dt.timedelta(hours=int(hours))

    limit = max(1, min(500, int(limit)))

    try:
        with make_session() as session:
            repo = Repo(session)
            t = None
            if topic:
                t = repo.get_topic_by_name(topic)
                if not t:
                    raise typer.BadParameter(f"topic not found: {topic}")
            rows = repo.list_recent_events(
                topic=t,
                decisions=[decision] if decision else None,
                since=since,
                limit=limit,
            )
    except OperationalError as exc:
        msg = str(getattr(exc, "orig", exc))
        if "no such table" in msg or "doesn't exist" in msg:
            console.print("[red]ERROR[/red] DB is not initialized (or TRACKER_DB_URL points to the wrong DB).")
            console.print("Hint: run `tracker db init` (and ensure you run from the project dir, or set TRACKER_DB_URL).")
            raise typer.Exit(1) from exc
        raise

    def _iso(value: dt.datetime | None) -> str | None:
        if value is None:
            return None
        return value.isoformat()

    if as_json:
        data = [
            {
                "id": it.id,
                "created_at": _iso(it.created_at),
                "topic_id": topic_row.id,
                "topic": topic_row.name,
                "decision": it.decision,
                "relevance_score": it.relevance_score,
                "novelty_score": it.novelty_score,
                "quality_score": it.quality_score,
                "reason": (it.reason or "")[:2000],
                "item_id": item.id,
                "item_title": item.title,
                "item_url": item.canonical_url,
                "item_published_at": _iso(item.published_at),
                "item_created_at": _iso(item.created_at),
                "source_id": source.id,
                "source_type": source.type,
                "source_url": source.url,
            }
            for it, item, topic_row, source in rows
        ]
        sys.stdout.write(json.dumps(data, ensure_ascii=False) + "\n")
        return

    if not rows:
        console.print("(no events)")
        return

    for it, item, topic_row, source in rows:
        when = (item.published_at or item.created_at).isoformat()
        title = (item.title or "").strip() or "(no title)"
        url = item.canonical_url
        scores = f"rel={it.relevance_score} nov={it.novelty_score} qual={it.quality_score}"
        reason = (it.reason or "").replace("\n", " ").strip()
        if len(reason) > 160:
            reason = reason[:160] + "…"

        line = f"- {when} ({it.decision}) {topic_row.name}: {title}"
        console.print(line, markup=False)
        console.print(f"  - url: {url}", markup=False)
        console.print(f"  - source: #{source.id} {source.type} {source.url}", markup=False)
        console.print(f"  - {scores}", markup=False)
        if reason:
            console.print(f"  - reason: {reason}", markup=False)


@report_app.command("list")
def report_list(
    kind: str | None = typer.Option(None, "--kind", help="Filter by kind: digest|health."),
    topic: str | None = typer.Option(None, "--topic", help="Filter by topic name (digest only)."),
    limit: int = typer.Option(20, "--limit", help="Max rows to return."),
    include_markdown: bool = typer.Option(False, "--with-markdown", help="Include markdown in JSON output."),
    as_json: bool = typer.Option(False, "--json", help="Output JSON to stdout."),
):
    """
    List archived reports (digests, health reports).
    """
    from sqlalchemy.exc import OperationalError

    valid_kinds = {"digest", "health"}
    if kind and kind not in valid_kinds:
        raise typer.BadParameter(f"invalid --kind: {kind} (expected one of: {', '.join(sorted(valid_kinds))})")

    settings = get_settings()
    _engine, make_session = session_factory(settings)
    limit = max(1, min(200, int(limit)))

    try:
        with make_session() as session:
            repo = Repo(session)
            t = None
            if topic:
                t = repo.get_topic_by_name(topic)
                if not t:
                    raise typer.BadParameter(f"topic not found: {topic}")
            rows = repo.list_reports(kind=kind, topic=t, limit=limit)
    except OperationalError as exc:
        msg = str(getattr(exc, "orig", exc))
        if "no such table" in msg or "doesn't exist" in msg:
            console.print("[red]ERROR[/red] DB is not initialized (or TRACKER_DB_URL points to the wrong DB).")
            console.print("Hint: run `tracker db init` (and ensure you run from the project dir, or set TRACKER_DB_URL).")
            raise typer.Exit(1) from exc
        raise

    if as_json:
        data = []
        for r, topic_row in rows:
            row = {
                "id": r.id,
                "kind": r.kind,
                "idempotency_key": r.idempotency_key,
                "topic_id": r.topic_id,
                "topic": topic_row.name if topic_row else None,
                "title": r.title,
                "created_at": r.created_at.isoformat(),
                "updated_at": r.updated_at.isoformat(),
            }
            if include_markdown:
                row["markdown"] = r.markdown
            data.append(row)
        sys.stdout.write(json.dumps(data, ensure_ascii=False) + "\n")
        return

    if not rows:
        console.print("(no reports)")
        return

    for r, topic_row in rows:
        topic_name = topic_row.name if topic_row else ""
        console.print(
            f"- #{r.id} {r.created_at.isoformat()} kind={r.kind} topic={topic_name} key={r.idempotency_key}",
            markup=False,
        )
        if r.title:
            console.print(f"  - title: {r.title}", markup=False)


@report_app.command("show")
def report_show(report_id: int = typer.Argument(..., help="Report ID.")):
    """
    Print the report markdown for an archived report.
    """
    from sqlalchemy.exc import OperationalError

    settings = get_settings()
    _engine, make_session = session_factory(settings)

    try:
        with make_session() as session:
            repo = Repo(session)
            r = repo.get_report_by_id(report_id)
            if not r:
                raise typer.BadParameter(f"report not found: {report_id}")
            console.print(r.markdown, markup=False)
    except OperationalError as exc:
        msg = str(getattr(exc, "orig", exc))
        if "no such table" in msg or "doesn't exist" in msg:
            console.print("[red]ERROR[/red] DB is not initialized (or TRACKER_DB_URL points to the wrong DB).")
            console.print("Hint: run `tracker db init` (and ensure you run from the project dir, or set TRACKER_DB_URL).")
            raise typer.Exit(1) from exc
        raise


@db_app.command("init")
def db_init():
    settings = get_settings()
    engine = create_engine_from_settings(settings)
    Base.metadata.create_all(engine)
    console.print(f"[green]OK[/green] DB initialized: {settings.db_url}")


@db_app.command("backup")
def db_backup():
    """
    Create a point-in-time SQLite backup (if using file-based SQLite).
    """
    from tracker.maintenance import run_backup

    settings = get_settings()
    out = run_backup(settings=settings)
    if out:
        console.print(f"[green]OK[/green] backup created: {out}")
    else:
        console.print(f"[yellow]SKIP[/yellow] backup not supported for db_url: {settings.db_url}")


@db_app.command("prune-ignored")
def db_prune_ignored(
    days: int = typer.Option(..., "--days", help="Delete ignored decisions older than N days."),
    apply: bool = typer.Option(False, "--apply", help="Apply changes (default is dry-run)."),
    keep_items: bool = typer.Option(False, "--keep-items", help="Do not delete orphan items."),
    vacuum: bool = typer.Option(False, "--vacuum", help="Run VACUUM after pruning (SQLite)."),
):
    import datetime as dt

    settings = get_settings()
    engine, make_session = session_factory(settings)

    older_than = dt.datetime.utcnow() - dt.timedelta(days=days)
    with make_session() as session:
        repo = Repo(session)
        result = repo.prune_ignored(
            older_than=older_than,
            delete_orphan_items=not keep_items,
            dry_run=not apply,
        )

    mode = "APPLY" if apply else "DRY-RUN"
    console.print(f"[cyan]{mode}[/cyan] prune_ignored older_than={older_than.isoformat()}Z")
    console.print(f"- item_topics_deleted: {result['item_topics_deleted']}")
    console.print(f"- items_deleted: {result['items_deleted']}")
    if not apply:
        console.print("Tip: re-run with `--apply` to delete.")
        return

    if vacuum and settings.db_url.startswith("sqlite:"):
        with engine.begin() as conn:
            conn.exec_driver_sql("VACUUM")
        console.print("[green]OK[/green] vacuum complete")


@topic_app.command("add")
def topic_add(
    name: str = typer.Option(..., "--name"),
    query: str = typer.Option("", "--query", help="Comma-separated keywords (v1)."),
    digest_cron: str = typer.Option("0 9 * * *", "--digest-cron"),
    alert_keywords: str = typer.Option("", "--alert-keywords", help="Comma-separated keywords (v1)."),
):
    settings = get_settings()
    _engine, make_session = session_factory(settings)
    with make_session() as session:
        topic = create_topic_action(
            session=session,
            spec=TopicSpec(
                name=name,
                query=query,
                digest_cron=digest_cron,
                alert_keywords=alert_keywords,
            ),
        )
    console.print(f"[green]OK[/green] topic id={topic.id} name={topic.name}")


@topic_app.command("bootstrap")
def topic_bootstrap(
    name: str = typer.Option(..., "--name"),
    query: str = typer.Option("", "--query", help="Comma-separated keywords (v1). Defaults to topic name if empty."),
    digest_cron: str = typer.Option("0 9 * * *", "--digest-cron"),
    alert_keywords: str = typer.Option("", "--alert-keywords", help="Comma-separated keywords (v1)."),
    searxng_base_url: str = typer.Option(
        "", "--searxng-base-url", help="If set, add a SearxNG search source bound to this topic."
    ),
    discourse_base_url: str = typer.Option(
        "", "--discourse-base-url", help="If set, add a Discourse source (e.g. https://forum.example.com)."
    ),
    discourse_json_path: str = typer.Option("/latest.json", "--discourse-json-path"),
    add_nodeseek: bool = typer.Option(
        False, "--add-nodeseek", help="If set, add NodeSeek RSS and bind it to this topic."
    ),
):
    """
    Create (or update) a topic, then seed a few “default” sources/bindings.

    Idempotent: safe to run multiple times.
    """
    q = query.strip() or name

    def _should_add_nodeseek_default(*, topic_name: str, topic_query: str) -> bool:
        text = f"{(topic_name or '').strip()}\n{(topic_query or '').strip()}".strip()
        if not text:
            return False
        low = text.lower()
        if "nodeseek" in low or "node seek" in low:
            return True
        return False

    settings = get_settings()
    _engine, make_session = session_factory(settings)
    with make_session() as session:
        repo = Repo(session)
        existing = repo.get_topic_by_name(name)
        if not existing:
            topic = create_topic_action(
                session=session,
                spec=TopicSpec(
                    name=name,
                    query=q,
                    digest_cron=digest_cron,
                    alert_keywords=alert_keywords,
                ),
            )
            console.print(f"[green]OK[/green] created topic id={topic.id} name={topic.name}")
        else:
            existing.query = q
            existing.digest_cron = digest_cron
            existing.alert_keywords = alert_keywords
            session.commit()
            console.print(f"[green]OK[/green] updated topic id={existing.id} name={existing.name}")

        hn = create_hn_search_source_action(
            session=session,
            query=q,
            bind=SourceBindingSpec(topic=name),
        )
        console.print(f"[green]OK[/green] hn_search source id={hn.id}")

        if searxng_base_url:
            sx = create_searxng_search_source_action(
                session=session,
                base_url=searxng_base_url,
                query=q,
                bind=SourceBindingSpec(topic=name),
            )
            console.print(f"[green]OK[/green] searxng_search source id={sx.id}")

        if discourse_base_url:
            disc = create_discourse_source_action(
                session=session,
                base_url=discourse_base_url,
                json_path=discourse_json_path,
                bind=SourceBindingSpec(topic=name, include_keywords=q),
            )
            console.print(f"[green]OK[/green] discourse source id={disc.id}")

        if add_nodeseek or _should_add_nodeseek_default(topic_name=name, topic_query=q):
            ns = create_rss_source_action(
                session=session,
                url="https://rss.nodeseek.com/",
                bind=SourceBindingSpec(topic=name, include_keywords=q),
            )
            console.print(f"[green]OK[/green] nodeseek rss source id={ns.id}")


@topic_app.command("bootstrap-file")
def topic_bootstrap_file(
    in_path: str = typer.Option(..., "--in", help="Topics file. Format: one per line `name|query` (query optional)."),
    digest_cron: str = typer.Option("0 9 * * *", "--digest-cron"),
    alert_keywords: str = typer.Option("", "--alert-keywords", help="Comma-separated keywords (v1)."),
    searxng_base_url: str = typer.Option(
        "", "--searxng-base-url", help="If set, add a SearxNG search source bound to each topic."
    ),
    discourse_base_url: str = typer.Option(
        "", "--discourse-base-url", help="If set, add a Discourse source (e.g. https://forum.example.com)."
    ),
    discourse_json_path: str = typer.Option("/latest.json", "--discourse-json-path"),
    add_nodeseek: bool = typer.Option(
        False, "--add-nodeseek", help="If set, add NodeSeek RSS and bind it to each topic."
    ),
):
    """
    Batch-create/update topics from a simple text file, then seed default sources/bindings.

    Idempotent: safe to re-run.
    """
    raw = Path(in_path).read_text(encoding="utf-8") if in_path != "-" else sys.stdin.read()
    rows: list[tuple[str, str]] = []
    for line in raw.splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        if "|" in s:
            name, query = s.split("|", 1)
            rows.append((name.strip(), query.strip()))
        else:
            rows.append((s, ""))

    if not rows:
        console.print("[yellow]SKIP[/yellow] no topics found in file")
        return

    settings = get_settings()
    _engine, make_session = session_factory(settings)
    created = 0
    updated = 0

    with make_session() as session:
        repo = Repo(session)
        for name, query in rows:
            if not name:
                continue
            q = query.strip() or name
            low = f"{name}\n{q}".lower()
            looks_china_dev = ("nodeseek" in low) or ("node seek" in low)
            existing = repo.get_topic_by_name(name)
            if not existing:
                create_topic_action(
                    session=session,
                    spec=TopicSpec(
                        name=name,
                        query=q,
                        digest_cron=digest_cron,
                        alert_keywords=alert_keywords,
                    ),
                )
                created += 1
            else:
                existing.query = q
                existing.digest_cron = digest_cron
                existing.alert_keywords = alert_keywords
                session.commit()
                updated += 1

            create_hn_search_source_action(
                session=session,
                query=q,
                bind=SourceBindingSpec(topic=name),
            )

            if searxng_base_url:
                create_searxng_search_source_action(
                    session=session,
                    base_url=searxng_base_url,
                    query=q,
                    bind=SourceBindingSpec(topic=name),
                )

            if discourse_base_url:
                create_discourse_source_action(
                    session=session,
                    base_url=discourse_base_url,
                    json_path=discourse_json_path,
                    bind=SourceBindingSpec(topic=name, include_keywords=q),
                )

            if add_nodeseek or looks_china_dev:
                create_rss_source_action(
                    session=session,
                    url="https://rss.nodeseek.com/",
                    bind=SourceBindingSpec(topic=name, include_keywords=q),
                )

    console.print(f"[green]OK[/green] bootstrap-file complete: created={created} updated={updated}")


@topic_app.command("list")
def topic_list():
    settings = get_settings()
    _engine, make_session = session_factory(settings)
    with make_session() as session:
        repo = Repo(session)
        for t in repo.list_topics():
            console.print(
                f"- {t.id} {t.name} enabled={t.enabled} digest={t.digest_cron} alerts={t.alert_keywords}"
            )


@topic_app.command("update")
def topic_update(
    name: str = typer.Argument(...),
    query: str | None = typer.Option(None, "--query"),
    digest_cron: str | None = typer.Option(None, "--digest-cron"),
    alert_keywords: str | None = typer.Option(None, "--alert-keywords"),
    alert_cooldown_minutes: int | None = typer.Option(None, "--alert-cooldown-minutes"),
    alert_daily_cap: int | None = typer.Option(None, "--alert-daily-cap"),
):
    settings = get_settings()
    _engine, make_session = session_factory(settings)
    with make_session() as session:
        repo = Repo(session)
        topic = repo.get_topic_by_name(name)
        if not topic:
            raise typer.BadParameter(f"topic not found: {name}")
        if query is not None:
            topic.query = query
        if digest_cron is not None:
            topic.digest_cron = digest_cron
        if alert_keywords is not None:
            topic.alert_keywords = alert_keywords
        if alert_cooldown_minutes is not None:
            topic.alert_cooldown_minutes = alert_cooldown_minutes
        if alert_daily_cap is not None:
            topic.alert_daily_cap = alert_daily_cap
        session.commit()
    console.print(f"[green]OK[/green] updated topic: {name}")


@topic_app.command("normalize-names")
def topic_normalize_names(
    apply: bool = typer.Option(False, "--apply", help="Apply changes (default is dry-run)."),
    include_profile: bool = typer.Option(False, "--include-profile", help="Also rename the Profile topic if applicable."),
    max_len: int = typer.Option(80, "--max-len", help="Max topic name length after normalization."),
):
    """
    Normalize long topic names into short display-friendly names.

    Example: "Axis Name：long explanation（...）" -> "Axis Name"
    """
    settings = get_settings()
    _engine, make_session = session_factory(settings)
    with make_session() as session:
        repo = Repo(session)

        profile_name = (repo.get_app_config("profile_topic_name") or "Profile").strip() or "Profile"

        def _normalize(name: str) -> str:
            raw = (name or "").strip()
            if not raw:
                return ""
            s = " ".join(raw.split()).strip()
            # Keep the short axis/topic name before ":" / "：".
            for sep in (":", "："):
                if sep in s:
                    left, _right = s.split(sep, 1)
                    left = left.strip()
                    if 2 <= len(left) <= 120:
                        s = left
                    break
            # Drop parenthetical tails.
            for open_br in ("（", "("):
                if open_br in s:
                    s = s.split(open_br, 1)[0].strip()
            s = s.strip() or raw.strip()
            if not s:
                return ""
            ml = max(10, min(200, int(max_len or 80)))
            if len(s) > ml:
                s = s[:ml].rstrip()
            return s

        topics = sorted(repo.list_topics(), key=lambda t: int(getattr(t, "id", 0) or 0))
        assigned_lower: set[str] = set()
        changes: list[tuple[int, str, str]] = []
        final_by_id: dict[int, str] = {}

        for t in topics:
            tid = int(getattr(t, "id", 0) or 0)
            old = (getattr(t, "name", "") or "").strip()
            if not old or tid <= 0:
                continue
            if (not include_profile) and old == profile_name:
                final_by_id[tid] = old
                assigned_lower.add(old.lower())
                continue

            desired = _normalize(old)
            if not desired:
                desired = old

            candidate = desired
            if candidate.lower() in assigned_lower and candidate != old:
                # Collision: add a suffix, keep stable order (by topic id).
                n = 2
                while True:
                    cand2 = f"{desired} ({n})"
                    if cand2.lower() not in assigned_lower:
                        candidate = cand2
                        break
                    n += 1
                    if n > 50:
                        candidate = f"{desired} ({tid})"
                        break

            final_by_id[tid] = candidate
            assigned_lower.add(candidate.lower())
            if candidate != old:
                changes.append((tid, old, candidate))

        if not changes:
            console.print("[green]OK[/green] no topic name changes needed")
            return

        for tid, old, new in changes[:500]:
            console.print(f"- {tid} {old} -> {new}", markup=False)
        if len(changes) > 500:
            console.print(f"... {len(changes) - 500} more", markup=False)

        if not apply:
            console.print(f"[yellow]DRY-RUN[/yellow] {len(changes)} changes. Re-run with --apply to write.", markup=False)
            return

        # Apply in one transaction.
        from tracker.models import Topic as TopicModel

        for tid, _old, new in changes:
            trow = session.get(TopicModel, tid)
            if not trow:
                continue
            trow.name = new
        session.commit()

        # If the Profile topic was renamed, update app_config pointer.
        if include_profile:
            for _tid, old, new in changes:
                if old == profile_name and new != old:
                    repo.set_app_config("profile_topic_name", new)
                    break

        console.print(f"[green]OK[/green] normalized topic names: updated={len(changes)}", markup=False)


@topic_app.command("merge-normalized")
def topic_merge_normalized(
    apply: bool = typer.Option(False, "--apply", help="Apply changes (default is dry-run)."),
    include_profile: bool = typer.Option(False, "--include-profile", help="Also merge the Profile topic if applicable."),
    max_len: int = typer.Option(80, "--max-len", help="Max topic name length after normalization."),
):
    """
    Merge duplicate topics that normalize to the same short name.

    This is useful when earlier installs created long "axis: description" topic names, and later
    runs created the short axis names as separate topics (leading to duplicated bindings/items).
    """
    from sqlalchemy import delete, select, tuple_, update

    from tracker.models import AlertBudget, ItemTopic, Report, SourceCandidate, Topic as TopicModel, TopicSource

    settings = get_settings()
    _engine, make_session = session_factory(settings)
    with make_session() as session:
        repo = Repo(session)

        profile_name = (repo.get_app_config("profile_topic_name") or "Profile").strip() or "Profile"

        def _normalize(name: str) -> str:
            raw = (name or "").strip()
            if not raw:
                return ""
            s = " ".join(raw.split()).strip()
            for sep in (":", "："):
                if sep in s:
                    left, _right = s.split(sep, 1)
                    left = left.strip()
                    if 2 <= len(left) <= 120:
                        s = left
                    break
            for open_br in ("（", "("):
                if open_br in s:
                    s = s.split(open_br, 1)[0].strip()
            s = s.strip() or raw.strip()
            if not s:
                return ""
            ml = max(10, min(200, int(max_len or 80)))
            if len(s) > ml:
                s = s[:ml].rstrip()
            return s

        topics = sorted(repo.list_topics(), key=lambda t: int(getattr(t, "id", 0) or 0))
        groups: dict[str, list[TopicModel]] = {}
        for t in topics:
            old = (getattr(t, "name", "") or "").strip()
            if not old:
                continue
            if (not include_profile) and old == profile_name:
                continue
            key = _normalize(old)
            if not key:
                continue
            groups.setdefault(key.casefold(), []).append(t)

        merges: list[tuple[str, TopicModel, list[TopicModel]]] = []
        for _k, ts in sorted(groups.items(), key=lambda kv: (len(kv[1]) * -1, kv[0])):
            if len(ts) <= 1:
                continue

            desired = _normalize((getattr(ts[0], "name", "") or "").strip())
            if not desired:
                continue

            # Pick merge target: prefer an exact desired-name match; otherwise shortest then lowest id.
            exact = [t for t in ts if (getattr(t, "name", "") or "").strip().casefold() == desired.casefold()]
            if exact:
                target = sorted(exact, key=lambda t: int(getattr(t, "id", 0) or 0))[0]
            else:
                target = sorted(
                    ts,
                    key=lambda t: (len((getattr(t, "name", "") or "").strip()), int(getattr(t, "id", 0) or 0)),
                )[0]

            others = [t for t in ts if int(getattr(t, "id", 0) or 0) != int(getattr(target, "id", 0) or 0)]
            if others:
                merges.append((desired, target, others))

        if not merges:
            console.print("[green]OK[/green] no normalized-topic merges needed")
            return

        for desired, target, others in merges[:200]:
            tname = (getattr(target, "name", "") or "").strip()
            oids = [int(getattr(o, "id", 0) or 0) for o in others]
            console.print(f"- merge into: #{int(target.id)} {tname} (normalized={desired!r}) from={oids}", markup=False)
        if len(merges) > 200:
            console.print(f"... {len(merges) - 200} more groups", markup=False)

        if not apply:
            console.print(f"[yellow]DRY-RUN[/yellow] groups={len(merges)}. Re-run with --apply to write.", markup=False)
            return

        def _merge_policy(*, src_topic_id: int, dst_topic_id: int) -> None:
            src_pol = repo.get_topic_policy(topic_id=src_topic_id)
            dst_pol = repo.get_topic_policy(topic_id=dst_topic_id)
            if not src_pol:
                return
            if not dst_pol:
                src_pol.topic_id = int(dst_topic_id)
                session.flush()
                return

            # Merge "enabled" as OR; prefer destination prompt unless it's empty.
            try:
                dst_pol.llm_curation_enabled = bool(dst_pol.llm_curation_enabled) or bool(src_pol.llm_curation_enabled)
            except Exception:
                pass
            try:
                if not (dst_pol.llm_curation_prompt or "").strip() and (src_pol.llm_curation_prompt or "").strip():
                    dst_pol.llm_curation_prompt = src_pol.llm_curation_prompt
            except Exception:
                pass
            session.delete(src_pol)
            session.flush()

        updated_groups = 0
        for _desired, target, others in merges:
            dst_id = int(getattr(target, "id", 0) or 0)
            if dst_id <= 0:
                continue

            for src in others:
                src_id = int(getattr(src, "id", 0) or 0)
                if src_id <= 0 or src_id == dst_id:
                    continue

                # Merge basic topic fields (best-effort; avoid clobbering destination config).
                try:
                    dst_row = session.get(TopicModel, dst_id)
                    src_row = session.get(TopicModel, src_id)
                    if dst_row and src_row:
                        if (not (dst_row.query or "").strip()) and (src_row.query or "").strip():
                            dst_row.query = src_row.query
                        if (not bool(dst_row.enabled)) and bool(src_row.enabled):
                            dst_row.enabled = True
                        if (not (dst_row.alert_keywords or "").strip()) and (src_row.alert_keywords or "").strip():
                            dst_row.alert_keywords = src_row.alert_keywords
                        if (dst_row.digest_cron or "").strip() == "0 9 * * *" and (src_row.digest_cron or "").strip() != "0 9 * * *":
                            dst_row.digest_cron = src_row.digest_cron
                except Exception:
                    pass

                _merge_policy(src_topic_id=src_id, dst_topic_id=dst_id)

                # Move bindings (topic_sources) with de-dup.
                target_source_ids = select(TopicSource.source_id).where(TopicSource.topic_id == dst_id)
                session.execute(
                    delete(TopicSource).where(
                        TopicSource.topic_id == src_id,
                        TopicSource.source_id.in_(target_source_ids),
                    )
                )
                session.execute(update(TopicSource).where(TopicSource.topic_id == src_id).values(topic_id=dst_id))

                # Move item_topics with de-dup.
                target_item_ids = select(ItemTopic.item_id).where(ItemTopic.topic_id == dst_id)
                session.execute(
                    delete(ItemTopic).where(
                        ItemTopic.topic_id == src_id,
                        ItemTopic.item_id.in_(target_item_ids),
                    )
                )
                session.execute(update(ItemTopic).where(ItemTopic.topic_id == src_id).values(topic_id=dst_id))

                # Move candidates with de-dup (unique on (topic_id, source_type, url)).
                target_pairs = select(SourceCandidate.source_type, SourceCandidate.url).where(SourceCandidate.topic_id == dst_id)
                session.execute(
                    delete(SourceCandidate).where(
                        SourceCandidate.topic_id == src_id,
                        tuple_(SourceCandidate.source_type, SourceCandidate.url).in_(target_pairs),
                    )
                )
                session.execute(update(SourceCandidate).where(SourceCandidate.topic_id == src_id).values(topic_id=dst_id))

                # Move alert budgets with de-dup (unique on (topic_id, day)).
                target_days = select(AlertBudget.day).where(AlertBudget.topic_id == dst_id)
                session.execute(
                    delete(AlertBudget).where(
                        AlertBudget.topic_id == src_id,
                        AlertBudget.day.in_(target_days),
                    )
                )
                session.execute(update(AlertBudget).where(AlertBudget.topic_id == src_id).values(topic_id=dst_id))

                # Move reports (best-effort).
                session.execute(update(Report).where(Report.topic_id == src_id).values(topic_id=dst_id))

                # Finally, delete the old topic row.
                try:
                    old_row = session.get(TopicModel, src_id)
                    if old_row:
                        session.delete(old_row)
                except Exception:
                    pass

                updated_groups += 1

        session.commit()
        console.print(f"[green]OK[/green] merged normalized topics: merges={updated_groups}", markup=False)


@topic_app.command("sync-search-sources")
def topic_sync_search_sources(
    name: str = typer.Option(..., "--name", help="Topic name."),
):
    """
    Sync bound HN/SearxNG search source queries to match the topic query.

    Useful after editing `topic.query`, so future candidates match your updated scope.
    """
    settings = get_settings()
    _engine, make_session = session_factory(settings)
    with make_session() as session:
        try:
            res = sync_topic_search_sources_action(session=session, topic_name=name)
        except ValueError as exc:
            raise typer.BadParameter(str(exc)) from exc

    console.print(
        f"[green]OK[/green] sync-search-sources: topic={name} updated={res.updated} created={res.created} rebound={res.rebound}"
    )


@topic_app.command("disable")
def topic_disable(name: str = typer.Argument(...)):
    settings = get_settings()
    _engine, make_session = session_factory(settings)
    with make_session() as session:
        set_topic_enabled_action(session=session, name=name, enabled=False)
    console.print(f"[green]OK[/green] disabled topic: {name}")


@topic_app.command("enable")
def topic_enable(name: str = typer.Argument(...)):
    settings = get_settings()
    _engine, make_session = session_factory(settings)
    with make_session() as session:
        set_topic_enabled_action(session=session, name=name, enabled=True)
    console.print(f"[green]OK[/green] enabled topic: {name}")


@topic_policy_app.command("show")
def topic_policy_show(
    topic: str = typer.Option(..., "--topic", help="Topic name."),
    as_json: bool = typer.Option(False, "--json", help="Output JSON to stdout."),
):
    settings = get_settings()
    engine, make_session = session_factory(settings)
    Base.metadata.create_all(engine)
    with make_session() as session:
        repo = Repo(session)
        t = repo.get_topic_by_name(topic)
        if not t:
            raise typer.BadParameter(f"topic not found: {topic}")
        pol = repo.get_topic_policy(topic_id=t.id)

        data = {
            "topic": t.name,
            "topic_id": t.id,
            "llm_curation_enabled": bool(pol.llm_curation_enabled) if pol else False,
            "llm_curation_prompt": (pol.llm_curation_prompt if pol else ""),
            "updated_at": (pol.updated_at.isoformat() if pol else None),
        }

    if as_json:
        sys.stdout.write(json.dumps(data, ensure_ascii=False) + "\n")
        return

    console.print(f"# Topic Policy: {data['topic']}")
    console.print(f"- llm_curation_enabled: {str(data['llm_curation_enabled']).lower()}")
    console.print("- llm_curation_prompt:")
    console.print(data["llm_curation_prompt"] or "(empty)", markup=False)


@topic_policy_app.command("set")
def topic_policy_set(
    topic: str = typer.Option(..., "--topic", help="Topic name."),
    enabled: bool = typer.Option(True, "--enabled/--disabled", help="Enable/disable LLM curation for this topic."),
    prompt: str | None = typer.Option(None, "--prompt", help="Prompt text (optional)."),
    prompt_file: Path | None = typer.Option(
        None, "--prompt-file", help="Read prompt from file (UTF-8). Use '-' to read from stdin."
    ),
):
    prompt_text: str | None = None
    if prompt_file is not None:
        if str(prompt_file) == "-":
            prompt_text = sys.stdin.read()
        else:
            prompt_text = Path(prompt_file).read_text(encoding="utf-8")
    elif prompt is not None:
        prompt_text = prompt

    settings = get_settings()
    engine, make_session = session_factory(settings)
    Base.metadata.create_all(engine)
    with make_session() as session:
        try:
            upsert_topic_ai_policy_action(
                session=session,
                spec=TopicAiPolicySpec(topic=topic, enabled=enabled, prompt=prompt_text),
            )
        except ValueError as exc:
            raise typer.BadParameter(str(exc)) from exc

    console.print(f"[green]OK[/green] policy updated: topic={topic} llm_curation_enabled={str(enabled).lower()}")


@source_app.command("add-rss")
def source_add_rss(
    url: str = typer.Option(..., "--url"),
    topic: str | None = typer.Option(None, "--topic", help="Bind to a topic name (optional)."),
    include_keywords: str = typer.Option("", "--include-keywords", help="Comma-separated keywords (v1)."),
    exclude_keywords: str = typer.Option("", "--exclude-keywords", help="Comma-separated keywords (v1)."),
):
    settings = get_settings()
    _engine, make_session = session_factory(settings)
    with make_session() as session:
        try:
            source = create_rss_source_action(
                session=session,
                url=url,
                bind=(
                    SourceBindingSpec(
                        topic=topic,
                        include_keywords=include_keywords,
                        exclude_keywords=exclude_keywords,
                    )
                    if topic
                    else None
                ),
            )
        except ValueError as exc:
            raise typer.BadParameter(str(exc)) from exc
        console.print(f"[green]OK[/green] source id={source.id} type=rss url={source.url}")


@source_app.command("add-nodeseek")
def source_add_nodeseek(
    topic: str | None = typer.Option(None, "--topic", help="Bind to a topic name (optional)."),
    include_keywords: str = typer.Option("", "--include-keywords", help="Comma-separated keywords (v1)."),
    exclude_keywords: str = typer.Option("", "--exclude-keywords", help="Comma-separated keywords (v1)."),
):
    """
    Convenience helper: add NodeSeek RSS as a source.
    """
    url = "https://rss.nodeseek.com/"
    settings = get_settings()
    _engine, make_session = session_factory(settings)
    with make_session() as session:
        try:
            source = create_rss_source_action(
                session=session,
                url=url,
                bind=(
                    SourceBindingSpec(
                        topic=topic,
                        include_keywords=include_keywords,
                        exclude_keywords=exclude_keywords,
                    )
                    if topic
                    else None
                ),
            )
        except ValueError as exc:
            raise typer.BadParameter(str(exc)) from exc
    console.print(f"[green]OK[/green] source id={source.id} type=rss url={source.url}")


@source_app.command("list-rss-packs")
def source_list_rss_packs():
    from tracker.source_packs import list_rss_packs

    packs = list_rss_packs()
    if not packs:
        console.print("[yellow]SKIP[/yellow] no packs available")
        return
    console.print("[bold]RSS packs[/bold]")
    for p in packs:
        console.print(f"- {p.id}: {p.label} (feeds={len(p.urls)})")


@source_app.command("add-rss-pack")
def source_add_rss_pack(
    pack: str = typer.Option(..., "--pack", help="RSS pack id (see: `tracker source list-rss-packs`)."),
    topic: str | None = typer.Option(None, "--topic", help="Bind to a topic name (optional)."),
    tags: str = typer.Option("", "--tags", help="Optional comma-separated tags to set on sources."),
    notes: str = typer.Option("", "--notes", help="Optional notes to set on sources."),
    include_keywords: str = typer.Option("", "--include-keywords", help="Comma-separated keywords (v1)."),
    exclude_keywords: str = typer.Option("", "--exclude-keywords", help="Comma-separated keywords (v1)."),
):
    from tracker.actions import SourceBindingSpec, create_rss_sources_bulk as create_rss_sources_bulk_action
    from tracker.source_packs import get_rss_pack

    settings = get_settings()
    _engine, make_session = session_factory(settings)
    with make_session() as session:
        try:
            p = get_rss_pack(pack)
        except ValueError as exc:
            raise typer.BadParameter(str(exc)) from exc
        try:
            created, bound = create_rss_sources_bulk_action(
                session=session,
                urls=p.urls,
                bind=(
                    SourceBindingSpec(
                        topic=topic,
                        include_keywords=include_keywords,
                        exclude_keywords=exclude_keywords,
                    )
                    if topic
                    else None
                ),
                tags=(tags.strip() or None),
                notes=(notes.strip() or None),
            )
        except ValueError as exc:
            raise typer.BadParameter(str(exc)) from exc
    console.print(
        f"[green]OK[/green] rss pack imported: id={p.id} feeds={len(p.urls)} created={created} bound={bound}"
    )


@source_app.command("normalize-search-queries")
def source_normalize_search_queries(
    apply: bool = typer.Option(False, "--apply", help="Apply changes (default is dry-run)."),
):
    """
    Normalize existing HN/SearxNG search source URLs.

    This rewrites comma-separated keyword queries into space-separated queries so search engines
    behave as expected (e.g. `a,b,c` → `a b c`).
    """
    from tracker.search_query import rewrite_query_param

    settings = get_settings()
    _engine, make_session = session_factory(settings)
    changes = 0
    scanned = 0

    with make_session() as session:
        repo = Repo(session)
        for s in repo.list_sources():
            scanned += 1
            param = ""
            if s.type == "hn_search":
                param = "query"
            elif s.type == "searxng_search":
                param = "q"
            else:
                continue

            new_url = rewrite_query_param(url=s.url, param=param)
            if new_url == s.url:
                continue

            changes += 1
            console.print(f"- source #{s.id} type={s.type}")
            console.print(f"  - old: {s.url}", markup=False)
            console.print(f"  - new: {new_url}", markup=False)
            if apply:
                s.url = new_url

        if apply and changes:
            session.commit()

    if not changes:
        console.print(f"[green]OK[/green] normalize-search-queries: no changes (scanned={scanned})")
        return

    if apply:
        console.print(f"[green]OK[/green] normalize-search-queries: updated {changes} sources")
    else:
        console.print(f"[yellow]DRY-RUN[/yellow] would update {changes} sources (re-run with --apply)")


@source_app.command("add-hn-search")
def source_add_hn_search(
    query: str = typer.Option(..., "--query", help="Search query string."),
    topic: str | None = typer.Option(None, "--topic", help="Bind to a topic name (optional)."),
    tags: str = typer.Option("story", "--tags", help="Algolia tags filter (default: story)."),
    hits_per_page: int = typer.Option(50, "--hits-per-page"),
    include_keywords: str = typer.Option("", "--include-keywords", help="Comma-separated keywords (v1)."),
    exclude_keywords: str = typer.Option("", "--exclude-keywords", help="Comma-separated keywords (v1)."),
):
    settings = get_settings()
    _engine, make_session = session_factory(settings)
    with make_session() as session:
        try:
            source = create_hn_search_source_action(
                session=session,
                query=query,
                tags=tags,
                hits_per_page=hits_per_page,
                bind=(
                    SourceBindingSpec(
                        topic=topic,
                        include_keywords=include_keywords,
                        exclude_keywords=exclude_keywords,
                    )
                    if topic
                    else None
                ),
            )
        except ValueError as exc:
            raise typer.BadParameter(str(exc)) from exc
        console.print(f"[green]OK[/green] source id={source.id} type=hn_search url={source.url}")


@source_app.command("add-searxng-search")
def source_add_searxng_search(
    base_url: str = typer.Option(..., "--base-url", help="SearxNG base URL (e.g. http://127.0.0.1:8888)."),
    query: str = typer.Option(..., "--query"),
    topic: str | None = typer.Option(None, "--topic", help="Bind to a topic name (optional)."),
    categories: str = typer.Option("", "--categories", help="Optional categories filter."),
    time_range: str = typer.Option("day", "--time-range", help="day|week|month|year (if supported)."),
    language: str = typer.Option("", "--language", help="Optional language code."),
    results: int = typer.Option(20, "--results", help="Max results (if supported)."),
    include_keywords: str = typer.Option("", "--include-keywords", help="Comma-separated keywords (v1)."),
    exclude_keywords: str = typer.Option("", "--exclude-keywords", help="Comma-separated keywords (v1)."),
):
    settings = get_settings()
    _engine, make_session = session_factory(settings)
    with make_session() as session:
        try:
            source = create_searxng_search_source_action(
                session=session,
                base_url=base_url,
                query=query,
                categories=categories,
                time_range=time_range,
                language=language,
                results=results,
                bind=(
                    SourceBindingSpec(
                        topic=topic,
                        include_keywords=include_keywords,
                        exclude_keywords=exclude_keywords,
                    )
                    if topic
                    else None
                ),
            )
        except ValueError as exc:
            raise typer.BadParameter(str(exc)) from exc
        console.print(f"[green]OK[/green] source id={source.id} type=searxng_search url={source.url}")


@source_app.command("add-discourse")
def source_add_discourse(
    base_url: str = typer.Option(..., "--base-url", help="Discourse base URL (e.g. https://forum.example.com)."),
    json_path: str = typer.Option("/latest.json", "--json-path", help="JSON listing path (default: /latest.json)."),
    topic: str | None = typer.Option(None, "--topic", help="Bind to a topic name (optional)."),
    include_keywords: str = typer.Option("", "--include-keywords", help="Comma-separated keywords (v1)."),
    exclude_keywords: str = typer.Option("", "--exclude-keywords", help="Comma-separated keywords (v1)."),
):
    settings = get_settings()
    _engine, make_session = session_factory(settings)
    with make_session() as session:
        try:
            source = create_discourse_source_action(
                session=session,
                base_url=base_url,
                json_path=json_path,
                bind=(
                    SourceBindingSpec(
                        topic=topic,
                        include_keywords=include_keywords,
                        exclude_keywords=exclude_keywords,
                    )
                    if topic
                    else None
                ),
            )
        except ValueError as exc:
            raise typer.BadParameter(str(exc)) from exc
        console.print(f"[green]OK[/green] source id={source.id} type=discourse url={source.url}")


@source_app.command("add-llm-models")
def source_add_llm_models(
    base_url: str = typer.Option("", "--base-url", help="OpenAI-compatible LLM gateway base URL (e.g. http://127.0.0.1:8317)."),
    topic: str | None = typer.Option(None, "--topic", help="Bind to a topic name (optional)."),
    include_keywords: str = typer.Option("", "--include-keywords", help="Comma-separated keywords (v1)."),
    exclude_keywords: str = typer.Option("", "--exclude-keywords", help="Comma-separated keywords (v1)."),
):
    """
    Poll `/v1/models` from your configured LLM gateway to detect newly available models.
    """
    settings = get_settings()
    u = (base_url or "").strip() or (settings.llm_base_url or "").strip()
    if not u:
        raise typer.BadParameter("missing --base-url (and TRACKER_LLM_BASE_URL is not set)")
    _engine, make_session = session_factory(settings)
    with make_session() as session:
        try:
            source = create_llm_models_source_action(
                session=session,
                base_url=u,
                bind=(
                    SourceBindingSpec(
                        topic=topic,
                        include_keywords=include_keywords,
                        exclude_keywords=exclude_keywords,
                    )
                    if topic
                    else None
                ),
            )
        except ValueError as exc:
            raise typer.BadParameter(str(exc)) from exc
        console.print(f"[green]OK[/green] source id={source.id} type=llm_models url={source.url}")


@source_app.command("add-html-list")
def source_add_html_list(
    page_url: str = typer.Option(..., "--page-url", help="Page URL to scrape (http(s):// or file://)."),
    item_selector: str = typer.Option(..., "--item-selector", help="CSS selector for list item nodes."),
    title_selector: str = typer.Option("a", "--title-selector", help="CSS selector (within item) for title node."),
    summary_selector: str = typer.Option("", "--summary-selector", help="CSS selector (within item) for summary node."),
    max_items: int = typer.Option(30, "--max-items", help="Max items per fetch (1..200)."),
    topic: str | None = typer.Option(None, "--topic", help="Bind to a topic name (optional)."),
    include_keywords: str = typer.Option("", "--include-keywords", help="Comma-separated keywords (v1)."),
    exclude_keywords: str = typer.Option("", "--exclude-keywords", help="Comma-separated keywords (v1)."),
):
    """
    Scrape a webpage listing and extract item links using CSS selectors.

    Stores the scraping spec in a `html-list://...` URL so it can be exported/imported safely.
    """
    settings = get_settings()
    _engine, make_session = session_factory(settings)
    with make_session() as session:
        try:
            source = create_html_list_source_action(
                session=session,
                page_url=page_url,
                item_selector=item_selector,
                title_selector=title_selector or None,
                summary_selector=summary_selector or None,
                max_items=max_items,
                bind=(
                    SourceBindingSpec(
                        topic=topic,
                        include_keywords=include_keywords,
                        exclude_keywords=exclude_keywords,
                    )
                    if topic
                    else None
                ),
            )
        except ValueError as exc:
            raise typer.BadParameter(str(exc)) from exc
        console.print(f"[green]OK[/green] source id={source.id} type=html_list url={source.url}")


@source_app.command("add-github-releases")
def source_add_github_releases(
    repo_name: str = typer.Option(..., "--repo", help="GitHub repo as owner/repo or https://github.com/owner/repo"),
    topic: str | None = typer.Option(None, "--topic", help="Bind to a topic name (optional)."),
    include_keywords: str = typer.Option("", "--include-keywords", help="Comma-separated keywords (v1)."),
    exclude_keywords: str = typer.Option("", "--exclude-keywords", help="Comma-separated keywords (v1)."),
):
    from tracker.connectors.github_atom import build_github_releases_atom_url

    settings = get_settings()
    _engine, make_session = session_factory(settings)
    with make_session() as session:
        repo = Repo(session)
        url = build_github_releases_atom_url(repo=repo_name)
        source = repo.add_source(type="rss", url=url)
        if topic:
            t = repo.get_topic_by_name(topic)
            if not t:
                raise typer.BadParameter(f"topic not found: {topic}")
            ts = repo.bind_topic_source(topic=t, source=source)
            ts.include_keywords = include_keywords
            ts.exclude_keywords = exclude_keywords
            session.commit()
        console.print(f"[green]OK[/green] source id={source.id} type=rss url={source.url}")


@source_app.command("add-github-issues")
def source_add_github_issues(
    repo_name: str = typer.Option(..., "--repo", help="GitHub repo as owner/repo or https://github.com/owner/repo"),
    topic: str | None = typer.Option(None, "--topic", help="Bind to a topic name (optional)."),
    include_keywords: str = typer.Option("", "--include-keywords", help="Comma-separated keywords (v1)."),
    exclude_keywords: str = typer.Option("", "--exclude-keywords", help="Comma-separated keywords (v1)."),
):
    from tracker.connectors.github_atom import build_github_issues_atom_url

    settings = get_settings()
    _engine, make_session = session_factory(settings)
    with make_session() as session:
        repo = Repo(session)
        url = build_github_issues_atom_url(repo=repo_name)
        source = repo.add_source(type="rss", url=url)
        if topic:
            t = repo.get_topic_by_name(topic)
            if not t:
                raise typer.BadParameter(f"topic not found: {topic}")
            ts = repo.bind_topic_source(topic=t, source=source)
            ts.include_keywords = include_keywords
            ts.exclude_keywords = exclude_keywords
            session.commit()
        console.print(f"[green]OK[/green] source id={source.id} type=rss url={source.url}")


@source_app.command("add-github-pulls")
def source_add_github_pulls(
    repo_name: str = typer.Option(..., "--repo", help="GitHub repo as owner/repo or https://github.com/owner/repo"),
    topic: str | None = typer.Option(None, "--topic", help="Bind to a topic name (optional)."),
    include_keywords: str = typer.Option("", "--include-keywords", help="Comma-separated keywords (v1)."),
    exclude_keywords: str = typer.Option("", "--exclude-keywords", help="Comma-separated keywords (v1)."),
):
    from tracker.connectors.github_atom import build_github_pulls_atom_url

    settings = get_settings()
    _engine, make_session = session_factory(settings)
    with make_session() as session:
        repo = Repo(session)
        url = build_github_pulls_atom_url(repo=repo_name)
        source = repo.add_source(type="rss", url=url)
        if topic:
            t = repo.get_topic_by_name(topic)
            if not t:
                raise typer.BadParameter(f"topic not found: {topic}")
            ts = repo.bind_topic_source(topic=t, source=source)
            ts.include_keywords = include_keywords
            ts.exclude_keywords = exclude_keywords
            session.commit()
        console.print(f"[green]OK[/green] source id={source.id} type=rss url={source.url}")


@source_app.command("add-github-commits")
def source_add_github_commits(
    repo_name: str = typer.Option(..., "--repo", help="GitHub repo as owner/repo or https://github.com/owner/repo"),
    branch: str = typer.Option("main", "--branch"),
    topic: str | None = typer.Option(None, "--topic", help="Bind to a topic name (optional)."),
    include_keywords: str = typer.Option("", "--include-keywords", help="Comma-separated keywords (v1)."),
    exclude_keywords: str = typer.Option("", "--exclude-keywords", help="Comma-separated keywords (v1)."),
):
    from tracker.connectors.github_atom import build_github_commits_atom_url

    settings = get_settings()
    _engine, make_session = session_factory(settings)
    with make_session() as session:
        repo = Repo(session)
        url = build_github_commits_atom_url(repo=repo_name, branch=branch)
        source = repo.add_source(type="rss", url=url)
        if topic:
            t = repo.get_topic_by_name(topic)
            if not t:
                raise typer.BadParameter(f"topic not found: {topic}")
            ts = repo.bind_topic_source(topic=t, source=source)
            ts.include_keywords = include_keywords
            ts.exclude_keywords = exclude_keywords
            session.commit()
        console.print(f"[green]OK[/green] source id={source.id} type=rss url={source.url}")


@source_app.command("preview")
def source_preview(
    source_id: int = typer.Argument(..., help="Source id."),
    limit: int = typer.Option(10, "--limit", help="Max entries to print."),
    as_json: bool = typer.Option(False, "--json", help="Print JSON."),
):
    """
    Fetch a source once and print extracted entries (no DB writes).
    """
    from sqlalchemy.exc import OperationalError

    from tracker.pipeline import fetch_entries_for_source
    from tracker.http_auth import cookie_header_for_url, parse_cookie_jar_json

    async def _main():
        settings = get_settings()
        cookie_jar = parse_cookie_jar_json(getattr(settings, "cookie_jar_json", "") or "")

        async def _cookie_cb(u: str) -> str | None:
            return cookie_header_for_url(url=u, cookie_jar=cookie_jar)

        _engine, make_session = session_factory(settings)
        with make_session() as session:
            repo = Repo(session)
            source = repo.get_source_by_id(source_id)
            if not source:
                raise ValueError(f"source not found: {source_id}")
            extra: dict[str, object] = {}
            if source.type == "llm_models":
                extra["llm_models_api_key"] = (settings.llm_api_key or None)
            entries = await fetch_entries_for_source(
                source=source,
                timeout_seconds=settings.http_timeout_seconds,
                cookie_header_cb=_cookie_cb,
                **extra,
            )
            return source, entries

    try:
        source, entries = asyncio.run(_main())
    except OperationalError as exc:
        msg = str(getattr(exc, "orig", exc))
        if "no such table" in msg or "doesn't exist" in msg:
            console.print("[red]ERROR[/red] DB is not initialized (or TRACKER_DB_URL points to the wrong DB).")
            console.print("Hint: run `tracker db init` (and ensure you run from the project dir, or set TRACKER_DB_URL).")
            raise typer.Exit(1) from exc
        raise
    except ValueError as exc:
        console.print(f"[red]ERROR[/red] {exc}")
        raise typer.Exit(2) from exc

    if as_json:
        payload = [
            {"url": e.url, "title": e.title, "published_at_iso": e.published_at_iso, "summary": e.summary}
            for e in entries[: max(0, limit)]
        ]
        sys.stdout.write(json.dumps({"source": {"id": source.id, "type": source.type, "url": source.url}, "entries": payload}, ensure_ascii=False, indent=2) + "\n")
        return

    console.print(f"# Source #{source.id} ({source.type})")
    console.print(source.url)
    for e in entries[: max(0, limit)]:
        title = (e.title or "").strip()
        console.print(f"- {title} {e.url}")


@source_app.command("discover-feeds")
def source_discover_feeds(
    page_url: str = typer.Option(..., "--page-url", help="Web page URL (http(s):// or file://)."),
):
    """
    Try to discover RSS/Atom feed URLs from a page.
    """
    from urllib.parse import urlparse

    import httpx

    from tracker.feed_discovery import discover_feed_urls_from_html

    parsed = urlparse(page_url)
    if parsed.scheme == "file":
        html = Path(parsed.path).read_text(encoding="utf-8")
    else:
        settings = get_settings()
        resp = httpx.get(
            page_url,
            headers={"User-Agent": "tracker/0.1"},
            timeout=settings.http_timeout_seconds,
            follow_redirects=True,
        )
        resp.raise_for_status()
        html = resp.text

    urls = discover_feed_urls_from_html(page_url=page_url, html=html)
    if not urls:
        console.print("[yellow]No feeds found.[/yellow]")
        raise typer.Exit(1)

    for u in urls:
        console.print(f"- {u}")


@source_app.command("discover-apis")
def source_discover_apis(
    page_url: str = typer.Option(..., "--page-url", help="Web page URL (http(s):// or file://)."),
    ai: bool = typer.Option(False, "--ai", help="Use the configured LLM to guess additional API endpoints."),
):
    """
    Try to discover likely public web API endpoints from a page.

    This is an operator helper; it never writes to the DB.
    """
    from urllib.parse import urlparse

    import httpx

    from tracker.api_discovery import discover_api_urls_from_html
    from tracker.llm import llm_guess_api_endpoints

    parsed = urlparse(page_url)
    if parsed.scheme == "file":
        html = Path(parsed.path).read_text(encoding="utf-8")
    else:
        settings = get_settings()
        resp = httpx.get(
            page_url,
            headers={"User-Agent": "tracker/0.1"},
            timeout=settings.http_timeout_seconds,
            follow_redirects=True,
        )
        resp.raise_for_status()
        html = resp.text

    urls = discover_api_urls_from_html(page_url=page_url, html=html)
    if ai:
        settings = get_settings()
        _engine, make_session = session_factory(settings)
        with make_session() as session:
            repo = Repo(session)
            try:
                from tracker.dynamic_config import effective_settings

                settings_eff = effective_settings(repo=repo, settings=settings)
            except Exception:
                settings_eff = settings
            guessed = asyncio.run(
                llm_guess_api_endpoints(settings=settings_eff, page_url=page_url, html_snippet=html)
            )
            if guessed:
                for u in guessed:
                    if u not in urls:
                        urls.append(u)

    if not urls:
        console.print("[yellow]No API endpoints found.[/yellow]")
        raise typer.Exit(1)

    for u in sorted(urls):
        console.print(f"- {u}")


@source_app.command("list")
def source_list(
    health: bool = typer.Option(False, "--health", help="Include health/backoff info."),
    meta: bool = typer.Option(False, "--meta", help="Include tags/notes."),
):
    settings = get_settings()
    _engine, make_session = session_factory(settings)
    with make_session() as session:
        repo = Repo(session)
        if not health and not meta:
            for s in repo.list_sources():
                console.print(f"- {s.id} {s.type} enabled={s.enabled} {s.url}")
            return

        for s, h, m in repo.list_sources_with_health_and_meta():
            errs = h.error_count if h else 0
            next_at = h.next_fetch_at.isoformat() if h and h.next_fetch_at else ""
            last_ok = h.last_success_at.isoformat() if h and h.last_success_at else ""
            last_checked = s.last_checked_at.isoformat() if s.last_checked_at else ""
            tags = m.tags if m else ""
            notes = m.notes if m else ""
            notes_short = (notes[:120] + "…") if len(notes) > 120 else notes

            parts = [f"- {s.id} {s.type} enabled={s.enabled}"]
            if health:
                parts.append(f"errs={errs} next={next_at} last_ok={last_ok} checked={last_checked}")
            if meta:
                parts.append(f"tags={tags!r} notes={notes_short!r}")
            parts.append(s.url)
            console.print(" ".join(parts))


@source_app.command("meta")
def source_meta(
    source_id: int = typer.Argument(...),
    tags: str | None = typer.Option(None, "--tags"),
    notes: str | None = typer.Option(None, "--notes"),
):
    settings = get_settings()
    _engine, make_session = session_factory(settings)
    with make_session() as session:
        update_source_meta_action(session=session, source_id=source_id, tags=tags, notes=notes)
    console.print(f"[green]OK[/green] updated source meta: {source_id}")


@source_app.command("disable")
def source_disable(source_id: int = typer.Argument(...)):
    settings = get_settings()
    _engine, make_session = session_factory(settings)
    with make_session() as session:
        Repo(session).set_source_enabled(source_id, False)
    console.print(f"[green]OK[/green] disabled source: {source_id}")


@source_app.command("enable")
def source_enable(source_id: int = typer.Argument(...)):
    settings = get_settings()
    _engine, make_session = session_factory(settings)
    with make_session() as session:
        Repo(session).set_source_enabled(source_id, True)
    console.print(f"[green]OK[/green] enabled source: {source_id}")


@bind_app.command("list")
def bind_list(topic: str | None = typer.Option(None, "--topic")):
    settings = get_settings()
    _engine, make_session = session_factory(settings)
    with make_session() as session:
        repo = Repo(session)
        t = repo.get_topic_by_name(topic) if topic else None
        if topic and not t:
            raise typer.BadParameter(f"topic not found: {topic}")
        rows = repo.list_topic_sources(topic=t)
        for tt, ss, ts in rows:
            console.print(
                f"- topic={tt.name} source_id={ss.id} type={ss.type} enabled={ss.enabled} "
                f"include={ts.include_keywords!r} exclude={ts.exclude_keywords!r} url={ss.url}"
            )


@bind_app.command("add")
def bind_add(
    topic: str = typer.Option(..., "--topic"),
    source_id: int = typer.Option(..., "--source-id"),
    include_keywords: str = typer.Option("", "--include-keywords"),
    exclude_keywords: str = typer.Option("", "--exclude-keywords"),
):
    settings = get_settings()
    _engine, make_session = session_factory(settings)
    with make_session() as session:
        try:
            create_binding_action(
                session=session,
                topic_name=topic,
                source_id=source_id,
                include_keywords=include_keywords,
                exclude_keywords=exclude_keywords,
            )
        except ValueError as exc:
            raise typer.BadParameter(str(exc)) from exc
    console.print(f"[green]OK[/green] bound topic={topic} source_id={source_id}")


@bind_app.command("remove")
def bind_remove(
    topic: str = typer.Option(..., "--topic"),
    source_id: int = typer.Option(..., "--source-id"),
):
    settings = get_settings()
    _engine, make_session = session_factory(settings)
    with make_session() as session:
        try:
            remove_binding_action(session=session, topic_name=topic, source_id=source_id)
        except ValueError as exc:
            raise typer.BadParameter(str(exc)) from exc
    console.print(f"[green]OK[/green] unbound topic={topic} source_id={source_id}")


@bind_app.command("update")
def bind_update(
    topic: str = typer.Option(..., "--topic"),
    source_id: int = typer.Option(..., "--source-id"),
    include_keywords: str | None = typer.Option(None, "--include-keywords"),
    exclude_keywords: str | None = typer.Option(None, "--exclude-keywords"),
):
    settings = get_settings()
    _engine, make_session = session_factory(settings)
    with make_session() as session:
        try:
            update_binding_action(
                session=session,
                topic_name=topic,
                source_id=source_id,
                include_keywords=include_keywords,
                exclude_keywords=exclude_keywords,
            )
        except ValueError as exc:
            raise typer.BadParameter(str(exc)) from exc
    console.print(f"[green]OK[/green] updated binding topic={topic} source_id={source_id}")


@run_app.command("tick")
def run_tick(
    push: bool = typer.Option(False, "--push", help="Push new alerts (if configured)."),
    drain_backlog: bool = typer.Option(False, "--drain-backlog", help="Manually include historical pending candidates in tick-time LLM curation."),
):
    """
    Ingest enabled topic+source pairs and store item decisions.
    v1: RSS + search + small-site connectors.
    """
    from tracker.runner import run_tick as run_tick_core
    from tracker.job_lock import job_lock

    async def _main():
        settings = get_settings()
        try:
            with job_lock(name="jobs", timeout_seconds=120):
                _engine, make_session = session_factory(settings)
                with make_session() as session:
                    result = await run_tick_core(session=session, settings=settings, push=push, drain_backlog=drain_backlog)
        except TimeoutError:
            console.print("[red]ERROR[/red] busy: another job is running (try again soon)")
            raise typer.Exit(2) from None

        for r in result.per_source:
            msg = f"- {r.topic_name} ← {r.source_url}: +{r.created} alerts_pushed={r.pushed_alerts}"
            if r.error:
                msg += f" error={r.error}"
            console.print(msg)

        console.print(
            f"[green]OK[/green] tick complete: +{result.total_created} new items; pushed_alerts={result.total_pushed_alerts}"
        )

    asyncio.run(_main())


@run_app.command("digest")
def run_digest(
    hours: int = typer.Option(24, "--hours"),
    push: bool = typer.Option(False, "--push", help="Push digest (if configured)."),
    force: bool = typer.Option(False, "--force", help="Force a new idempotency key (resend push)."),
):
    from tracker.runner import run_curated_info as run_curated_info_core
    from tracker.job_lock import job_lock

    async def _main():
        settings = get_settings()
        try:
            with job_lock(name="jobs", timeout_seconds=120):
                _engine, make_session = session_factory(settings)
                with make_session() as session:
                    suffix = None
                    if force:
                        from tracker.push_ops import make_manual_key_suffix

                        suffix = make_manual_key_suffix()
                    result = await run_curated_info_core(session=session, settings=settings, hours=hours, push=push, key_suffix=suffix)
        except TimeoutError:
            console.print("[red]ERROR[/red] busy: another job is running (try again soon)")
            raise typer.Exit(2) from None

        console.print(result.markdown)
        if push:
            extra = f" key={result.idempotency_key}" if getattr(result, "idempotency_key", "") else ""
            console.print(f"- pushed={result.pushed}{extra}")

    asyncio.run(_main())

@run_app.command("health")
def run_health(
    push: bool = typer.Option(False, "--push", help="Push report (if configured)."),
):
    from tracker.runner import run_health_report
    from tracker.job_lock import job_lock

    async def _main():
        settings = get_settings()
        try:
            with job_lock(name="jobs", timeout_seconds=120):
                _engine, make_session = session_factory(settings)
                with make_session() as session:
                    result = await run_health_report(session=session, settings=settings, push=push)
        except TimeoutError:
            console.print("[red]ERROR[/red] busy: another job is running (try again soon)")
            raise typer.Exit(2) from None

        console.print(result.markdown)
        if push:
            console.print(f"- pushed={result.pushed}")

    asyncio.run(_main())


@run_app.command("discover-sources")
def run_discover_sources(
    topic: str | None = typer.Option(None, "--topic", help="Only discover for one topic (by name)."),
    max_results_per_topic: int | None = typer.Option(
        None, "--max-results-per-topic", help="Override TRACKER_DISCOVER_SOURCES_MAX_RESULTS_PER_TOPIC."
    ),
):
    """
    Discover RSS/Atom feeds from web-wide results and store them as candidates.
    """
    from tracker.runner import run_discover_sources as run_discover_sources_core
    from tracker.job_lock import job_lock

    async def _main():
        settings = get_settings()
        if max_results_per_topic is not None:
            settings.discover_sources_max_results_per_topic = max(1, int(max_results_per_topic))
        try:
            with job_lock(name="jobs", timeout_seconds=120):
                _engine, make_session = session_factory(settings)
                with make_session() as session:
                    repo = Repo(session)
                    topic_ids = None
                    if topic:
                        t = repo.get_topic_by_name(topic)
                        if not t:
                            raise typer.BadParameter(f"topic not found: {topic}")
                        topic_ids = [t.id]
                    result = await run_discover_sources_core(session=session, settings=settings, topic_ids=topic_ids)
        except TimeoutError:
            console.print("[red]ERROR[/red] busy: another job is running (try again soon)")
            raise typer.Exit(2) from None

        for r in result.per_topic:
            console.print(
                f"- {r.topic_name}: pages={r.pages_checked} candidates_created={r.candidates_created} "
                f"candidates_found={r.candidates_found} errors={r.errors}"
            )

        console.print("[green]OK[/green] discover-sources complete")

    asyncio.run(_main())


@service_app.command("run")
def service_run():
    """
    Run the long-lived scheduler: periodic tick (alerts) + per-topic daily digest cron.
    """
    from tracker.service import serve_forever

    asyncio.run(serve_forever())


@api_app.command("serve")
def api_serve(
    host: str = typer.Option(None, "--host", help="Bind host (defaults to TRACKER_API_HOST)."),
    port: int = typer.Option(None, "--port", help="Bind port (defaults to TRACKER_API_PORT)."),
    reload: bool = typer.Option(False, "--reload"),
):
    """
    Serve HTTP API + minimal admin UI (FastAPI).
    """
    import uvicorn

    settings = get_settings()
    uvicorn.run(
        "tracker.api:app",
        host=host or settings.api_host,
        port=port or settings.api_port,
        reload=reload,
    )


@config_app.command("export")
def config_export(out: str | None = typer.Option(None, "--out", help="Write to file (default: stdout).")):
    from tracker.config_io import export_config

    settings = get_settings()
    _engine, make_session = session_factory(settings)
    with make_session() as session:
        cfg = export_config(session=session)

    text = json.dumps(cfg, ensure_ascii=False, indent=2) + "\n"
    if out:
        Path(out).write_text(text, encoding="utf-8")
        console.print(f"[green]OK[/green] exported config: {out}")
    else:
        sys.stdout.write(text)


@config_app.command("import")
def config_import(
    in_path: str = typer.Option("-", "--in", help="JSON file path (default: stdin)."),
    update_existing: bool = typer.Option(False, "--update-existing", help="Update existing topics/sources/bindings."),
):
    from tracker.config_io import import_config

    if in_path == "-":
        data = json.loads(sys.stdin.read() or "{}")
    else:
        data = json.loads(Path(in_path).read_text(encoding="utf-8") or "{}")

    settings = get_settings()
    _engine, make_session = session_factory(settings)
    with make_session() as session:
        result = import_config(session=session, data=data, update_existing=update_existing)

    console.print("[green]OK[/green] import complete")
    for k in sorted(result.keys()):
        console.print(f"- {k}: {result[k]}")


@push_app.command("test")
def push_test(
    only: str | None = typer.Option(
        None, "--only", help="Only test one channel: dingtalk|telegram|email|webhook."
    ),
):
    """
    Send a small test message to the configured push channels.
    """
    from sqlalchemy.exc import OperationalError

    from tracker.push_ops import push_test as push_test_core

    valid = {"dingtalk", "telegram", "email", "webhook"}
    if only and only not in valid:
        raise typer.BadParameter(f"invalid --only: {only} (expected one of: {', '.join(sorted(valid))})")

    async def _main() -> list[tuple[str, str]]:
        settings = get_settings()
        _engine, make_session = session_factory(settings)
        with make_session() as session:
            return await push_test_core(session=session, settings=settings, only=only)

    try:
        results = asyncio.run(_main())
    except OperationalError as exc:
        msg = str(getattr(exc, "orig", exc))
        if "no such table" in msg or "doesn't exist" in msg:
            console.print("[red]ERROR[/red] DB is not initialized (or TRACKER_DB_URL points to the wrong DB).")
            console.print("Hint: run `tracker db init` (and ensure you run from the project dir, or set TRACKER_DB_URL).")
            raise typer.Exit(1) from exc
        raise

    for channel, status in results:
        console.print(f"- {channel}: {status}")


@push_app.command("list")
def push_list(
    channel: str | None = typer.Option(None, "--channel", help="Filter by channel: dingtalk|telegram|email|webhook."),
    status: str | None = typer.Option(None, "--status", help="Filter by status: pending|sent|failed."),
    key: str | None = typer.Option(None, "--key", help="Filter by idempotency key."),
    limit: int = typer.Option(50, "--limit", help="Max rows to return."),
    as_json: bool = typer.Option(False, "--json", help="Output JSON to stdout."),
):
    """
    List push attempts (idempotent delivery logs).
    """
    from sqlalchemy.exc import OperationalError

    valid_channels = {"dingtalk", "telegram", "email", "webhook"}
    valid_status = {"pending", "sent", "failed"}
    if channel and channel not in valid_channels:
        raise typer.BadParameter(
            f"invalid --channel: {channel} (expected one of: {', '.join(sorted(valid_channels))})"
        )
    if status and status not in valid_status:
        raise typer.BadParameter(
            f"invalid --status: {status} (expected one of: {', '.join(sorted(valid_status))})"
        )

    settings = get_settings()
    _engine, make_session = session_factory(settings)
    limit = max(1, min(200, int(limit)))

    try:
        with make_session() as session:
            rows = Repo(session).list_pushes(channel=channel, status=status, idempotency_key=key, limit=limit)
    except OperationalError as exc:
        msg = str(getattr(exc, "orig", exc))
        if "no such table" in msg or "doesn't exist" in msg:
            console.print("[red]ERROR[/red] DB is not initialized (or TRACKER_DB_URL points to the wrong DB).")
            console.print("Hint: run `tracker db init` (and ensure you run from the project dir, or set TRACKER_DB_URL).")
            raise typer.Exit(1) from exc
        raise

    if as_json:
        data = [
            {
                "id": p.id,
                "channel": p.channel,
                "idempotency_key": p.idempotency_key,
                "status": p.status,
                "attempts": p.attempts,
                "error": p.error,
                "created_at": p.created_at.isoformat(),
                "sent_at": p.sent_at.isoformat() if p.sent_at else None,
            }
            for p in rows
        ]
        sys.stdout.write(json.dumps(data, ensure_ascii=False) + "\n")
        return

    if not rows:
        console.print("(no pushes)")
        return

    for p in rows:
        sent = p.sent_at.isoformat() if p.sent_at else ""
        console.print(
            f"- #{p.id} {p.created_at.isoformat()} channel={p.channel} status={p.status} attempts={p.attempts} sent_at={sent}",
            markup=False,
        )
        console.print(f"  - key: {p.idempotency_key}", markup=False)
        if p.error:
            console.print(f"  - error: {p.error}", markup=False)


@push_app.command("retry")
def push_retry(
    idempotency_key: str = typer.Option(..., "--key", help="Idempotency key to retry."),
    only: str | None = typer.Option(None, "--only", help="Only retry one channel: dingtalk|telegram|email|webhook."),
):
    """
    Retry a push by idempotency key (digest/alert/health).
    """
    from sqlalchemy.exc import OperationalError

    from tracker.push_ops import retry_push_key

    async def _main() -> list[tuple[str, str]]:
        settings = get_settings()
        _engine, make_session = session_factory(settings)
        with make_session() as session:
            result = await retry_push_key(
                session=session,
                settings=settings,
                idempotency_key=idempotency_key,
                only=only,
            )
        return result.results

    try:
        results = asyncio.run(_main())
    except OperationalError as exc:
        msg = str(getattr(exc, "orig", exc))
        if "no such table" in msg or "doesn't exist" in msg:
            console.print("[red]ERROR[/red] DB is not initialized (or TRACKER_DB_URL points to the wrong DB).")
            console.print("Hint: run `tracker db init` (and ensure you run from the project dir, or set TRACKER_DB_URL).")
            raise typer.Exit(1) from exc
        raise
    except ValueError as exc:
        console.print(f"[red]ERROR[/red] {exc}")
        raise typer.Exit(2) from exc

    for channel, status in results:
        console.print(f"- {channel}: {status}")


@push_app.command("retry-failed")
def push_retry_failed(
    limit: int = typer.Option(20, "--limit", help="Max unique keys to retry."),
):
    """
    Retry recently failed pushes (digest/alert/health).
    """
    from sqlalchemy.exc import OperationalError

    from tracker.push_ops import retry_failed_pushes

    async def _main():
        settings = get_settings()
        _engine, make_session = session_factory(settings)
        with make_session() as session:
            return await retry_failed_pushes(session=session, settings=settings, max_keys=limit)

    try:
        results = asyncio.run(_main())
    except OperationalError as exc:
        msg = str(getattr(exc, "orig", exc))
        if "no such table" in msg or "doesn't exist" in msg:
            console.print("[red]ERROR[/red] DB is not initialized (or TRACKER_DB_URL points to the wrong DB).")
            console.print("Hint: run `tracker db init` (and ensure you run from the project dir, or set TRACKER_DB_URL).")
            raise typer.Exit(1) from exc
        raise

    if not results:
        console.print("(no failed pushes to retry)")
        return

    for r in results:
        console.print(f"- {r.idempotency_key}", markup=False)
        for channel, status in r.results:
            console.print(f"  - {channel}: {status}", markup=False)


@candidate_app.command("list")
def candidate_list(
    topic: str | None = typer.Option(None, "--topic", help="Filter by topic name."),
    status: str = typer.Option("new", "--status", help="Filter by status: new|accepted|ignored."),
    limit: int = typer.Option(50, "--limit"),
):
    settings = get_settings()
    _engine, make_session = session_factory(settings)
    with make_session() as session:
        repo = Repo(session)
        t = repo.get_topic_by_name(topic) if topic else None
        if topic and not t:
            raise typer.BadParameter(f"topic not found: {topic}")
        rows = repo.list_source_candidates(topic=t, status=status or None, limit=limit)
        for cand, tt in rows:
            console.print(
                f"- #{cand.id} topic={tt.name} status={cand.status} type={cand.source_type} "
                f"seen={cand.seen_count} last={cand.last_seen_at.isoformat()} url={cand.url}"
            )


@candidate_app.command("preview")
def candidate_preview(
    candidate_id: int = typer.Argument(..., help="Candidate id."),
    limit: int = typer.Option(10, "--limit", help="Max entries to print."),
    as_json: bool = typer.Option(False, "--json", help="Print JSON."),
):
    """
    Fetch a candidate feed once and print extracted entries (no DB writes).
    """
    from sqlalchemy.exc import OperationalError

    from tracker.connectors.rss import RssConnector
    from tracker.models import Topic

    async def _main():
        settings = get_settings()
        _engine, make_session = session_factory(settings)
        with make_session() as session:
            repo = Repo(session)
            cand = repo.get_source_candidate_by_id(candidate_id)
            if not cand:
                raise ValueError(f"candidate not found: {candidate_id}")
            if (cand.source_type or "").strip().lower() != "rss":
                raise ValueError(f"unsupported candidate type for preview: {cand.source_type}")
            topic = session.get(Topic, cand.topic_id)
            topic_name = topic.name if topic else str(cand.topic_id)
            entries = await RssConnector(timeout_seconds=settings.http_timeout_seconds).fetch(url=cand.url)
            return cand, topic_name, entries

    try:
        cand, topic_name, entries = asyncio.run(_main())
    except OperationalError as exc:
        msg = str(getattr(exc, "orig", exc))
        if "no such table" in msg or "doesn't exist" in msg:
            console.print("[red]ERROR[/red] DB is not initialized (or TRACKER_DB_URL points to the wrong DB).")
            console.print("Hint: run `tracker db init` (and ensure you run from the project dir, or set TRACKER_DB_URL).")
            raise typer.Exit(1) from exc
        raise
    except ValueError as exc:
        console.print(f"[red]ERROR[/red] {exc}")
        raise typer.Exit(2) from exc

    if as_json:
        payload = [
            {"url": e.url, "title": e.title, "published_at_iso": e.published_at_iso, "summary": e.summary}
            for e in entries[: max(0, limit)]
        ]
        sys.stdout.write(
            json.dumps(
                {
                    "candidate": {
                        "id": cand.id,
                        "topic_id": cand.topic_id,
                        "topic": topic_name,
                        "type": cand.source_type,
                        "url": cand.url,
                    },
                    "entries": payload,
                },
                ensure_ascii=False,
                indent=2,
            )
            + "\n"
        )
        return

    console.print(f"# Candidate #{cand.id} topic={topic_name} ({cand.source_type})")
    console.print(cand.url)
    for e in entries[: max(0, limit)]:
        title = (e.title or "").strip()
        console.print(f"- {title} {e.url}")


@candidate_app.command("accept")
def candidate_accept(
    candidate_id: int = typer.Argument(...),
    enabled: bool = typer.Option(True, "--enabled/--disabled", help="Whether to enable the created source."),
):
    settings = get_settings()
    _engine, make_session = session_factory(settings)
    with make_session() as session:
        try:
            source = accept_source_candidate_action(session=session, candidate_id=candidate_id, enabled=enabled)
        except ValueError as exc:
            console.print(f"[red]ERROR[/red] {exc}")
            raise typer.Exit(2) from exc
    console.print(f"[green]OK[/green] accepted candidate #{candidate_id} -> source #{source.id} ({source.type})")


@candidate_app.command("ignore")
def candidate_ignore(candidate_id: int = typer.Argument(...)):
    settings = get_settings()
    _engine, make_session = session_factory(settings)
    with make_session() as session:
        try:
            ignore_source_candidate_action(session=session, candidate_id=candidate_id)
        except ValueError as exc:
            console.print(f"[red]ERROR[/red] {exc}")
            raise typer.Exit(2) from exc
    console.print(f"[green]OK[/green] ignored candidate: {candidate_id}")


@candidate_app.command("cleanup")
def candidate_cleanup(
    apply: bool = typer.Option(False, "--apply", help="Apply changes (default is dry-run)."),
    limit: int = typer.Option(500, "--limit"),
):
    """
    Cleanup low-signal candidates in bulk (currently: comment feeds).
    """
    from tracker.feed_discovery import looks_like_comment_feed_url

    settings = get_settings()
    _engine, make_session = session_factory(settings)
    with make_session() as session:
        repo = Repo(session)
        rows = repo.list_source_candidates(status="new", limit=limit)
        targets = [(cand, tt) for cand, tt in rows if looks_like_comment_feed_url(cand.url)]

        if not targets:
            console.print("(no comment-feed candidates)")
            return

        if not apply:
            for cand, tt in targets:
                console.print(f"- #{cand.id} topic={tt.name} url={cand.url}")
            console.print("Run with `--apply` to mark them as ignored.")
            return

        for cand, _tt in targets:
            cand.status = "ignored"
        session.commit()
        console.print(f"[green]OK[/green] ignored {len(targets)} comment-feed candidates")
