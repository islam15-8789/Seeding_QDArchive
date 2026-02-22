"""Command-line interface for the QDArchive data harvester."""

import logging
from datetime import datetime
from pathlib import Path

import click
import httpx
from rich.console import Console
from rich.table import Table

from harvester.database.engine import open_session, setup_database
from harvester.database.export import write_csv
from harvester.database.models import File
from harvester.helpers.licensing import license_is_open
from harvester.helpers.log import init_logging
from harvester.settings import (
    EXCLUDED_RESOURCE_TYPES,
    FOLDER_NAMES,
    OUTPUT_DIR,
    QDA_FORMATS,
    QUALITATIVE_FORMATS,
    RELEVANCE_KEYWORDS,
    ROOT_DIR,
    prepare_directories,
)
from harvester.sources import SOURCES
from harvester.storage.files import build_output_path, sha256_digest

terminal = Console()
log = logging.getLogger("harvester")


@click.group()
def app() -> None:
    """QDArchive Harvester — collect open qualitative research data."""
    prepare_directories()
    setup_database()
    init_logging()


# ── Helpers ─────────────────────────────────────────────────────


def _resolve_source(key: str):
    """Look up a source by its short key, or abort."""
    src = SOURCES.get(key)
    if src is None:
        valid = ", ".join(SOURCES)
        terminal.print(f"[red]Unknown source '{key}'. Available: {valid}[/red]")
        raise SystemExit(1)
    return src


def _store_metadata_record(
    session, source_name, hit, meta, finfo, fname, ext, qda_flag,
    folder=None, notes="access restricted",
):
    """Persist a record where the file itself was not downloaded."""
    already = (
        session.query(File)
        .filter_by(source_name=source_name, download_url=finfo["download_url"], file_name=fname)
        .first()
    )
    if already:
        return

    rec = File(
        source_name=source_name,
        source_url=hit.source_url,
        download_url=finfo["download_url"],
        file_name=fname,
        file_type=ext,
        file_size_bytes=finfo.get("size"),
        local_path=None,
        local_directory=folder,
        license_type=meta.license_type,
        license_url=meta.license_url,
        title=meta.title,
        description=meta.description,
        authors=meta.authors,
        date_published=meta.date_published,
        tags="; ".join(meta.tags) if meta.tags else None,
        keywords="; ".join(meta.keywords) if meta.keywords else None,
        kind_of_data="; ".join(meta.kind_of_data) if meta.kind_of_data else None,
        language="; ".join(meta.language) if meta.language else None,
        software="; ".join(meta.software) if meta.software else None,
        geographic_coverage=(
            "; ".join(meta.geographic_coverage) if meta.geographic_coverage else None
        ),
        content_type=finfo.get("content_type"),
        friendly_type=finfo.get("friendly_type"),
        restricted=finfo.get("restricted", False),
        api_checksum=finfo.get("api_checksum"),
        depositor=meta.depositor or None,
        producer="; ".join(meta.producer) if meta.producer else None,
        publication="; ".join(meta.publication) if meta.publication else None,
        date_of_collection=meta.date_of_collection or None,
        time_period_covered=meta.time_period_covered or None,
        uploader_name=meta.uploader_name or None,
        uploader_email=meta.uploader_email or None,
        is_qda_file=qda_flag,
        notes=notes,
    )
    session.add(rec)
    session.commit()


def _is_qda_file(fname: str, finfo: dict) -> bool:
    """Decide whether a file is a QDA artefact based on extension or API hints."""
    ext = Path(fname).suffix.lower()
    label = finfo.get("friendly_type", "").lower()
    mime = finfo.get("content_type", "").lower()
    return ext in QDA_FORMATS or "refi-qda" in label or "refiqda" in mime


def _dataset_has_qda(files: list[dict]) -> bool:
    """Return True when at least one file in the dataset looks like a QDA file."""
    return any(_is_qda_file(f["name"], f) for f in files)


# ── Core harvesting loop ───────────────────────────────────────


_DEFAULT_SIZE_CAP = 100 * 1024 * 1024  # 100 MB


def _process_hits(source, source_name, hits, session, size_cap=_DEFAULT_SIZE_CAP):
    """Walk through search hits: fetch metadata, check license, download.

    Returns a triple (downloaded, restricted, skipped).
    """
    n_downloaded = 0
    n_restricted = 0
    n_skipped = 0

    for idx, hit in enumerate(hits, 1):
        terminal.print(f"\n[bold][{idx}/{len(hits)}][/bold] {hit.title[:70]}")

        try:
            meta = source.fetch_metadata(hit.source_url)
        except Exception as exc:
            terminal.print(f"  [red]Could not fetch metadata: {exc}[/red]")
            continue

        # Only keep openly-licensed datasets
        if not license_is_open(meta.license_type):
            lic_display = (meta.license_type or "none")[:120]
            if len(meta.license_type or "") > 120:
                lic_display += "…"
            terminal.print(
                f"  [yellow]Skipped — license not open: "
                f"'{lic_display}'[/yellow]"
            )
            n_skipped += 1
            continue

        if not meta.files:
            terminal.print("  [yellow]No files attached to this dataset.[/yellow]")
            continue

        # Exclude non-data resource types unless a QDA file is present
        if meta.kind_of_data:
            kinds_lower = {v.strip().lower() for v in meta.kind_of_data}
            if kinds_lower & EXCLUDED_RESOURCE_TYPES:
                if not _dataset_has_qda(meta.files):
                    terminal.print(
                        f"  [dim]Skipped — non-data resource: "
                        f"'{'; '.join(meta.kind_of_data)}'[/dim]"
                    )
                    n_skipped += 1
                    continue

        # Relevance gate: description + keywords must contain a qualitative term
        has_qda = _dataset_has_qda(meta.files)
        if not has_qda:
            combined_text = (meta.description or "").lower()
            if meta.keywords:
                combined_text += " " + " ".join(k.lower() for k in meta.keywords)
            if not any(kw in combined_text for kw in RELEVANCE_KEYWORDS):
                terminal.print("  [dim]Skipped — no qualitative signal in description[/dim]")
                n_skipped += 1
                continue

        # Process each file in the dataset
        for finfo in meta.files:
            fname = finfo["name"]
            file_url = finfo["download_url"]
            ext = Path(fname).suffix.lower()
            qda_flag = _is_qda_file(fname, finfo)

            # Only download recognised formats; everything else → metadata only
            if not qda_flag and ext not in QUALITATIVE_FORMATS:
                _store_metadata_record(
                    session, source_name, hit, meta, finfo,
                    fname, ext, qda_flag, folder=None,
                    notes="irrelevant file type",
                )
                terminal.print(f"  [dim]{fname} ({ext}) — metadata only (not qualitative)[/dim]")
                continue

            # Determine storage path
            if "persistentId=" in hit.source_url:
                rid = hit.source_url.split("persistentId=")[-1]
            else:
                rid = str(finfo["id"])
            rid = rid.replace("/", "_").replace(":", "_")

            dir_label = FOLDER_NAMES.get(source_name, source_name)
            dest_path = build_output_path(dir_label, rid, fname, title=meta.title)
            dest_dir = str(dest_path.parent)
            folder_name = dest_path.parent.name

            # Duplicate check by URL
            existing = (
                session.query(File)
                .filter_by(source_name=source_name, download_url=file_url)
                .first()
            )
            if existing:
                terminal.print(f"  [dim]Already recorded: {fname}[/dim]")
                continue

            # Skip non-QDA files larger than the size cap (QDA files always download)
            file_bytes = finfo.get("size", 0) or 0
            if size_cap and file_bytes > size_cap and not qda_flag:
                _store_metadata_record(
                    session, source_name, hit, meta, finfo,
                    fname, ext, qda_flag, folder=folder_name,
                    notes=f"oversized ({file_bytes / (1024*1024):.0f} MB)",
                )
                terminal.print(
                    f"  [yellow]{fname} ({file_bytes / (1024*1024):.0f} MB) "
                    f"— skipped, exceeds size cap[/yellow]"
                )
                n_skipped += 1
                continue

            # If the API flags the file as restricted, save metadata only
            if finfo.get("restricted", False):
                _store_metadata_record(
                    session, source_name, hit, meta, finfo,
                    fname, ext, qda_flag, folder=folder_name,
                )
                n_restricted += 1
                tag = "[green]QDA[/green]" if qda_flag else "[dim]file[/dim]"
                terminal.print(f"  {tag} {fname} [yellow](restricted — metadata saved)[/yellow]")
                continue

            # Attempt the actual download
            try:
                local = source.pull_file(file_url, dest_dir, filename=fname)
            except httpx.HTTPStatusError as err:
                if err.response.status_code == 403:
                    _store_metadata_record(
                        session, source_name, hit, meta, finfo,
                        fname, ext, qda_flag, folder=folder_name,
                    )
                    n_restricted += 1
                    tag = "[green]QDA[/green]" if qda_flag else "[dim]file[/dim]"
                    terminal.print(
                        f"  {tag} {fname} [yellow](restricted — metadata saved)[/yellow]"
                    )
                    continue
                terminal.print(f"  [red]Download error for {fname}: {err}[/red]")
                continue
            except Exception as err:
                terminal.print(f"  [red]Download error for {fname}: {err}[/red]")
                continue

            local_size = Path(local).stat().st_size
            if local_size > 50 * 1024 * 1024:
                terminal.print(
                    f"  [dim]Hashing {local_size / (1024*1024):.0f} MB…[/dim]"
                )
            digest = sha256_digest(Path(local))

            # Hash-based deduplication
            dup = session.query(File).filter_by(file_hash=digest).first()
            if dup:
                terminal.print(f"  [dim]Duplicate (hash): {fname}[/dim]")
                Path(local).unlink(missing_ok=True)
                continue

            rec = File(
                source_name=source_name,
                source_url=hit.source_url,
                download_url=file_url,
                file_name=fname,
                file_type=ext,
                file_hash=digest,
                file_size_bytes=finfo.get("size"),
                local_path=str(Path(local).relative_to(ROOT_DIR)),
                local_directory=folder_name,
                license_type=meta.license_type,
                license_url=meta.license_url,
                title=meta.title,
                description=meta.description,
                authors=meta.authors,
                date_published=meta.date_published,
                tags="; ".join(meta.tags) if meta.tags else None,
                keywords="; ".join(meta.keywords) if meta.keywords else None,
                kind_of_data="; ".join(meta.kind_of_data) if meta.kind_of_data else None,
                language="; ".join(meta.language) if meta.language else None,
                software="; ".join(meta.software) if meta.software else None,
                geographic_coverage=(
                    "; ".join(meta.geographic_coverage) if meta.geographic_coverage else None
                ),
                content_type=finfo.get("content_type"),
                friendly_type=finfo.get("friendly_type"),
                restricted=finfo.get("restricted", False),
                api_checksum=finfo.get("api_checksum"),
                depositor=meta.depositor or None,
                producer="; ".join(meta.producer) if meta.producer else None,
                publication="; ".join(meta.publication) if meta.publication else None,
                date_of_collection=meta.date_of_collection or None,
                time_period_covered=meta.time_period_covered or None,
                uploader_name=meta.uploader_name or None,
                uploader_email=meta.uploader_email or None,
                is_qda_file=qda_flag,
                downloaded_at=datetime.utcnow(),
            )
            session.add(rec)
            session.commit()
            n_downloaded += 1

            tag = "[green]QDA[/green]" if qda_flag else "[blue]file[/blue]"
            terminal.print(f"  {tag} {fname} ({finfo.get('size', '?')} bytes)")

    return n_downloaded, n_restricted, n_skipped


# ── Reusable orchestration helpers ─────────────────────────────


def _read_query_list(path: str | None, single: str | None) -> list[str]:
    """Build a list of search terms from a file, a single value, or the default."""
    if path:
        return [
            ln.strip()
            for ln in Path(path).read_text().splitlines()
            if ln.strip() and not ln.strip().startswith("#")
        ]
    if single:
        return [single]
    return ["qualitative"]


def _run_source(
    source, source_name: str, queries: list[str], cap: int | None,
    size_cap: int = _DEFAULT_SIZE_CAP,
) -> tuple[int, int, int]:
    """Execute every query against one source. Returns (downloaded, restricted, skipped)."""
    session = open_session()
    tot_dl = 0
    tot_rest = 0
    tot_skip = 0
    seen: set[str] = set()

    try:
        for qi, q in enumerate(queries, 1):
            terminal.print(f"\n[bold]=== Query {qi}/{len(queries)}: '{q}' ===[/bold]")
            terminal.print(f"[dim]Searching {source_name}…[/dim]")

            try:
                hits = source.find(q)
            except Exception as exc:
                terminal.print(f"[red]Search error: {exc}[/red]")
                continue

            # Drop datasets already seen in earlier queries
            hits = [h for h in hits if h.source_url not in seen]
            seen.update(h.source_url for h in hits)

            if cap:
                hits = hits[:cap]

            terminal.print(f"Found {len(hits)} new dataset(s).")
            if not hits:
                continue

            dl, rest, skip = _process_hits(source, source_name, hits, session, size_cap)
            tot_dl += dl
            tot_rest += rest
            tot_skip += skip

    finally:
        session.close()

    return tot_dl, tot_rest, tot_skip


# ── CLI commands ───────────────────────────────────────────────


@app.command()
@click.argument("source")
@click.option("--query", "-q", default="qualitative", help="Search term.")
@click.option("--file-type", "-t", default=None, help="Restrict to a file extension.")
def find(source: str, query: str, file_type: str | None) -> None:
    """Search a source and display matching datasets."""
    src = _resolve_source(source)

    terminal.print(f"[bold]Searching {source}[/bold] for '{query}'…")
    try:
        hits = src.find(query, file_type)
    except Exception as exc:
        terminal.print(f"[red]Search failed: {exc}[/red]")
        raise SystemExit(1) from exc

    if not hits:
        terminal.print("[yellow]Nothing found.[/yellow]")
        return

    tbl = Table(title=f"Results from {source} ({len(hits)} datasets)")
    tbl.add_column("#", style="dim", width=4)
    tbl.add_column("Title", max_width=60)
    tbl.add_column("Authors", max_width=30)
    tbl.add_column("Published", width=12)

    for i, h in enumerate(hits, 1):
        tbl.add_row(
            str(i),
            h.title[:60],
            h.authors[:30] if h.authors else "",
            h.date_published[:10] if h.date_published else "",
        )

    terminal.print(tbl)


@app.command()
@click.argument("source")
@click.option("--limit", "-n", default=None, type=int, help="Cap datasets per query.")
@click.option("--query", "-q", default=None, help="Single search term.")
@click.option(
    "--queries-file", "-f", default=None,
    type=click.Path(exists=True),
    help="File containing one query per line.",
)
@click.option(
    "--max-file-size", "-m", default=100, type=int,
    help="Skip files larger than this (MB). 0 = no limit.",
)
def harvest(
    source: str, limit: int | None, query: str | None, queries_file: str | None,
    max_file_size: int,
) -> None:
    """Harvest data from a single source."""
    src = _resolve_source(source)
    queries = _read_query_list(queries_file, query)
    size_cap = max_file_size * 1024 * 1024 if max_file_size else 0

    dl, rest, skip = _run_source(src, source, queries, limit, size_cap)

    terminal.print(
        f"\n[bold]Finished.[/bold] Queries: {len(queries)}, "
        f"Downloaded: {dl}, Restricted: {rest}, Skipped: {skip}"
    )


@app.command("collect-all")
@click.option(
    "--queries-file", "-f", default=None,
    type=click.Path(exists=True),
    help="File with one query per line (default: queries.txt in project root).",
)
@click.option("--limit", "-n", default=None, type=int, help="Cap datasets per query per source.")
@click.option("--retries", "-r", default=1, type=int, help="Retry attempts for failed sources.")
@click.option(
    "--max-file-size", "-m", default=100, type=int,
    help="Skip files larger than this (MB). 0 = no limit.",
)
def collect_all(
    queries_file: str | None, limit: int | None, retries: int, max_file_size: int,
) -> None:
    """Harvest every configured source in sequence with retry."""
    # Fall back to queries.txt if it exists
    if queries_file is None:
        default = ROOT_DIR / "queries.txt"
        if default.exists():
            queries_file = str(default)

    queries = _read_query_list(queries_file, None)
    size_cap = max_file_size * 1024 * 1024 if max_file_size else 0
    terminal.print(
        f"[bold]Collecting from {len(SOURCES)} source(s) — "
        f"{len(queries)} queries, limit={limit or 'none'}, retries={retries}, "
        f"max file size={max_file_size or 'unlimited'} MB[/bold]\n"
    )

    results: dict[str, dict] = {}
    failures: list[str] = []

    for key, src in SOURCES.items():
        terminal.print(f"\n[bold cyan]>>> {key}[/bold cyan]")
        try:
            dl, rest, skip = _run_source(src, key, queries, limit, size_cap)
            results[key] = {
                "status": "OK", "downloaded": dl,
                "restricted": rest, "skipped": skip, "error": None,
            }
        except Exception as exc:
            log.exception("Source %s failed entirely", key)
            terminal.print(f"[red]{key} failed: {exc}[/red]")
            results[key] = {
                "status": "FAILED", "downloaded": 0,
                "restricted": 0, "skipped": 0, "error": str(exc),
            }
            failures.append(key)

    # Retry loop
    for attempt in range(1, retries + 1):
        if not failures:
            break
        terminal.print(
            f"\n[bold yellow]Retrying {len(failures)} source(s) "
            f"(attempt {attempt}/{retries})[/bold yellow]"
        )
        still_broken: list[str] = []
        for key in failures:
            src = SOURCES[key]
            terminal.print(f"\n[bold cyan]>>> Retry: {key}[/bold cyan]")
            try:
                dl, rest, skip = _run_source(src, key, queries, limit, size_cap)
                results[key] = {
                    "status": "OK", "downloaded": dl,
                    "restricted": rest, "skipped": skip, "error": None,
                }
            except Exception as exc:
                log.exception("Retry %d for %s failed", attempt, key)
                terminal.print(f"[red]{key} retry failed: {exc}[/red]")
                results[key]["error"] = str(exc)
                still_broken.append(key)
        failures = still_broken

    _show_collection_report(results)


def _show_collection_report(results: dict[str, dict]) -> None:
    """Render a Rich summary table after collect-all finishes."""
    tbl = Table(title="Collection Report")
    tbl.add_column("Source", style="bold", width=14)
    tbl.add_column("Status", width=8)
    tbl.add_column("Downloaded", justify="right", width=11)
    tbl.add_column("Restricted", justify="right", width=11)
    tbl.add_column("Skipped", justify="right", width=8)
    tbl.add_column("Error", max_width=40)

    sum_dl = sum_rest = sum_skip = 0
    ok = fail = 0

    for key, info in results.items():
        style = "[green]OK[/green]" if info["status"] == "OK" else "[red]FAILED[/red]"
        tbl.add_row(
            key, style,
            str(info["downloaded"]), str(info["restricted"]),
            str(info["skipped"]), info["error"] or "",
        )
        sum_dl += info["downloaded"]
        sum_rest += info["restricted"]
        sum_skip += info["skipped"]
        if info["status"] == "OK":
            ok += 1
        else:
            fail += 1

    terminal.print()
    terminal.print(tbl)
    terminal.print(
        f"\n[bold]Summary:[/bold] {ok} succeeded, {fail} failed | "
        f"Downloaded: {sum_dl}, Restricted: {sum_rest}, Skipped: {sum_skip}"
    )


# ── Database browsing ──────────────────────────────────────────


@app.command("dump")
@click.option("--format", "fmt", default="csv", type=click.Choice(["csv"]), help="Output format.")
@click.option("--output", "-o", default=None, help="Destination file path.")
def dump_cmd(fmt: str, output: str | None) -> None:
    """Export the metadata database to a file."""
    if output is None:
        output = str(OUTPUT_DIR / f"metadata.{fmt}")

    count = write_csv(Path(output))
    terminal.print(f"Exported {count} record(s) to {output}")


@app.command()
@click.option("--yes", "-y", is_flag=True, help="Skip the confirmation prompt.")
def wipe(yes: bool) -> None:
    """Delete all data: database, downloads, exports, and logs."""
    import shutil

    from harvester.settings import DATABASE_PATH, LOG_PATH

    if not yes:
        if not click.confirm("This will erase the database, downloads, exports, and logs. Sure?"):
            terminal.print("[dim]Cancelled.[/dim]")
            return

    gone = []
    from harvester.settings import DOWNLOAD_DIR as dl_dir

    if DATABASE_PATH.exists():
        DATABASE_PATH.unlink()
        gone.append(f"Database: {DATABASE_PATH}")
    if dl_dir.exists():
        shutil.rmtree(dl_dir)
        gone.append(f"Downloads: {dl_dir}")
    if OUTPUT_DIR.exists():
        shutil.rmtree(OUTPUT_DIR)
        gone.append(f"Output: {OUTPUT_DIR}")
    if LOG_PATH.exists():
        LOG_PATH.unlink()
        gone.append(f"Log: {LOG_PATH}")

    prepare_directories()
    setup_database()

    for item in gone:
        terminal.print(f"  Removed {item}")
    if not gone:
        terminal.print("  Nothing to remove.")
    terminal.print("[bold]Wipe complete.[/bold]")


@app.command()
def overview() -> None:
    """Display collection statistics and breakdowns."""
    session = open_session()
    try:
        total = session.query(File).count()
        qda = session.query(File).filter(File.is_qda_file.is_(True)).count()
        downloaded = session.query(File).filter(File.local_path.isnot(None)).count()

        from sqlalchemy import case, func

        restricted = session.query(File).filter(File.restricted.is_(True)).count()
        metadata_only = total - downloaded - restricted

        terminal.print(f"[bold]Total records:[/bold]    {total}")
        terminal.print(f"[bold]QDA files:[/bold]        {qda}")
        terminal.print()
        terminal.print(f"  [green]Downloaded:[/green]     {downloaded}")
        terminal.print(f"  [yellow]Restricted:[/yellow]     {restricted}  (metadata only)")
        terminal.print(f"  [dim]Other:[/dim]          {metadata_only}  (metadata only)")

        # Reusable aggregation expressions
        c_total = func.count(File.id).label("total")
        c_qda = func.sum(case((File.is_qda_file.is_(True), 1), else_=0)).label("qda")
        c_dl = func.sum(case((File.local_path.isnot(None), 1), else_=0)).label("downloaded")
        c_rest = func.sum(
            case((File.restricted.is_(True), 1), else_=0)
        ).label("restricted")

        def _breakdown(heading: str, rows: list, width: int = 30) -> None:
            if not rows:
                return
            terminal.print(f"\n[bold]{heading}[/bold]")
            hdr = f"  {'':>{width}}  {'Total':>7}  {'QDA':>5}  {'Down':>7}  {'Restr':>7}"
            terminal.print(f"[dim]{hdr}[/dim]")
            for label, t, q, d, r in rows:
                terminal.print(f"  {label:>{width}}  {t:>7}  {q:>5}  {d:>7}  {r:>7}")

        # Per-source
        by_source = (
            session.query(File.source_name, c_total, c_qda, c_dl, c_rest)
            .group_by(File.source_name)
            .order_by(c_total.desc())
            .all()
        )
        _breakdown("By source:", by_source, width=20)

        # Language
        by_lang = (
            session.query(File.language, c_total, c_qda, c_dl, c_rest)
            .filter(File.language.isnot(None))
            .group_by(File.language)
            .order_by(c_total.desc())
            .limit(10)
            .all()
        )
        _breakdown("By language:", by_lang, width=35)

        # Software
        by_sw = (
            session.query(File.software, c_total, c_qda, c_dl, c_rest)
            .filter(File.software.isnot(None))
            .group_by(File.software)
            .order_by(c_total.desc())
            .all()
        )
        _breakdown("By software:", by_sw, width=35)

        # File type
        by_ext = (
            session.query(File.file_type, c_total, c_qda, c_dl, c_rest)
            .filter(File.file_type.isnot(None))
            .group_by(File.file_type)
            .order_by(c_total.desc())
            .all()
        )
        _breakdown("By file type:", by_ext, width=20)

        # License
        by_lic = (
            session.query(File.license_type, c_total, c_qda, c_dl, c_rest)
            .filter(File.license_type.isnot(None))
            .group_by(File.license_type)
            .order_by(c_total.desc())
            .all()
        )
        _breakdown("By license:", by_lic, width=35)

    finally:
        session.close()


@app.command("browse")
@click.option("--source", "-s", default=None, help="Filter by source key.")
@click.option("--qda-only", is_flag=True, help="Only show QDA files.")
@click.option("--restricted-only", is_flag=True, help="Only show restricted records.")
@click.option("--search", default=None, help="Free-text search across title, description, keywords."
)
@click.option("--language", default=None, help="Substring filter on language field.")
@click.option("--software", default=None, help="Substring filter on software field.")
@click.option("--file-type", "file_type", default=None, help="Exact extension filter (e.g. .pdf).")
@click.option("--has-software", is_flag=True, help="Only records with software info.")
@click.option("--has-keywords", is_flag=True, help="Only records with keywords.")
@click.option("--limit", "-n", default=50, type=int, help="Maximum rows to show.")
def browse(
    source: str | None,
    qda_only: bool,
    restricted_only: bool,
    search: str | None,
    language: str | None,
    software: str | None,
    file_type: str | None,
    has_software: bool,
    has_keywords: bool,
    limit: int,
) -> None:
    """Browse stored records in a table view."""
    session = open_session()
    try:
        from sqlalchemy import or_

        q = session.query(File)
        if source:
            q = q.filter(File.source_name == source)
        if qda_only:
            q = q.filter(File.is_qda_file.is_(True))
        if restricted_only:
            q = q.filter(File.restricted.is_(True))
        if search:
            pat = f"%{search}%"
            q = q.filter(or_(
                File.title.ilike(pat),
                File.description.ilike(pat),
                File.keywords.ilike(pat),
                File.tags.ilike(pat),
            ))
        if language:
            q = q.filter(File.language.ilike(f"%{language}%"))
        if software:
            q = q.filter(File.software.ilike(f"%{software}%"))
        if file_type:
            ft = file_type if file_type.startswith(".") else f".{file_type}"
            q = q.filter(File.file_type == ft)
        if has_software:
            q = q.filter(File.software.isnot(None))
        if has_keywords:
            q = q.filter(File.keywords.isnot(None))

        total = q.count()
        rows = q.order_by(File.id).limit(limit).all()

        if not rows:
            terminal.print("[yellow]No matching records.[/yellow]")
            return

        tbl = Table(title=f"Records ({total} total, showing {len(rows)})")
        tbl.add_column("ID", style="dim", width=5)
        tbl.add_column("File", max_width=40)
        tbl.add_column("Type", width=6)
        tbl.add_column("Source", width=10)
        tbl.add_column("QDA", width=4)
        tbl.add_column("Status", width=12)
        tbl.add_column("Size", width=10, justify="right")

        for r in rows:
            if r.local_path:
                status = "[green]downloaded[/green]"
            elif r.notes and "restricted" in r.notes:
                status = "[yellow]restricted[/yellow]"
            else:
                status = "[dim]metadata[/dim]"

            size = _human_size(r.file_size_bytes) if r.file_size_bytes else ""
            qda_mark = "[green]yes[/green]" if r.is_qda_file else ""

            tbl.add_row(
                str(r.id), r.file_name[:40], r.file_type or "",
                r.source_name, qda_mark, status, size,
            )

        terminal.print(tbl)

        if total > limit:
            terminal.print(f"[dim]{limit} of {total} shown — increase --limit to see more[/dim]")

    finally:
        session.close()


@app.command("detail")
@click.argument("ids", nargs=-1, required=True, type=int)
def detail(ids: tuple[int, ...]) -> None:
    """Show every field for one or more records by ID."""
    session = open_session()
    try:
        for rid in ids:
            r = session.query(File).filter_by(id=rid).first()
            if not r:
                terminal.print(f"[red]No record with ID {rid}.[/red]")
                continue

            from rich.panel import Panel

            if r.local_path:
                status = "downloaded"
            elif r.notes and "restricted" in r.notes:
                status = "restricted"
            else:
                status = "metadata only"

            size = _human_size(r.file_size_bytes) if r.file_size_bytes else "unknown"

            lines = [
                f"[bold]File:[/bold]        {r.file_name}",
                f"[bold]Type:[/bold]        {r.file_type or 'unknown'}",
                f"[bold]Size:[/bold]        {size}",
                f"[bold]QDA file:[/bold]    {'yes' if r.is_qda_file else 'no'}",
                f"[bold]Status:[/bold]      {status}",
                f"[bold]Restricted:[/bold]  {'yes' if r.restricted else 'no'}",
                "",
                f"[bold]Title:[/bold]       {r.title or '—'}",
                f"[bold]Authors:[/bold]     {r.authors or '—'}",
                f"[bold]Uploader:[/bold]    {r.uploader_name or '—'}",
                f"[bold]Uploader email:[/bold] {r.uploader_email or '—'}",
                f"[bold]Published:[/bold]   {r.date_published or '—'}",
                f"[bold]Tags:[/bold]        {r.tags or '—'}",
                f"[bold]Keywords:[/bold]    {r.keywords or '—'}",
                f"[bold]Kind of data:[/bold] {r.kind_of_data or '—'}",
                f"[bold]Language:[/bold]    {r.language or '—'}",
                f"[bold]Software:[/bold]    {r.software or '—'}",
                f"[bold]Geography:[/bold]   {r.geographic_coverage or '—'}",
                f"[bold]Depositor:[/bold]   {r.depositor or '—'}",
                f"[bold]Producer:[/bold]    {r.producer or '—'}",
                f"[bold]Publication:[/bold] {r.publication or '—'}",
                f"[bold]Collection:[/bold]  {r.date_of_collection or '—'}",
                f"[bold]Time period:[/bold] {r.time_period_covered or '—'}",
                "",
                f"[bold]Source:[/bold]      {r.source_name}",
                f"[bold]Source URL:[/bold]  {r.source_url or '—'}",
                f"[bold]Download URL:[/bold] {r.download_url or '—'}",
                f"[bold]License:[/bold]     {r.license_type or '—'}",
                f"[bold]License URL:[/bold] {r.license_url or '—'}",
                "",
                f"[bold]Content type:[/bold] {r.content_type or '—'}",
                f"[bold]Friendly type:[/bold] {r.friendly_type or '—'}",
                f"[bold]Local dir:[/bold]   {r.local_directory or '—'}",
                f"[bold]Local path:[/bold]  {r.local_path or '—'}",
                f"[bold]File hash:[/bold]   {r.file_hash or '—'}",
                f"[bold]API checksum:[/bold] {r.api_checksum or '—'}",
                f"[bold]Downloaded:[/bold]  {r.downloaded_at or '—'}",
                f"[bold]Created:[/bold]     {r.created_at}",
                f"[bold]Notes:[/bold]       {r.notes or '—'}",
            ]

            desc = r.description or ""
            if desc:
                if len(desc) > 300:
                    desc = desc[:300] + "..."
                lines.insert(7, f"[bold]Description:[/bold] {desc}")

            terminal.print(Panel("\n".join(lines), title=f"Record #{r.id}", expand=False))

    finally:
        session.close()


@app.command("sources")
def list_sources() -> None:
    """Show configured data sources."""
    terminal.print("[bold]Configured sources:[/bold]\n")
    for key, src in SOURCES.items():
        terminal.print(f"  {key:<15} {src.label:<40} [green]active[/green]")


def _human_size(n: int) -> str:
    """Format a byte count for display."""
    if n < 1024:
        return f"{n} B"
    if n < 1024 * 1024:
        return f"{n / 1024:.1f} KB"
    return f"{n / (1024 * 1024):.1f} MB"


if __name__ == "__main__":
    app()
