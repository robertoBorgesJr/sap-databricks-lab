"""
Upload generated SAP data files to the Databricks landing volume.

Reads from the local output directory produced by `python -m generator full`
or `python -m generator delta`, and uploads every Parquet file to the
Unity Catalog Volume at /Volumes/{catalog}/landing/sap_raw/{TABLE}/.

Directory structure is preserved, so batch=N subdirectories created by the
CDC simulator are uploaded as-is and Auto Loader picks them up correctly.

The _state/ directory (internal generator state) is never uploaded.

Usage examples
--------------
  # Initial load: all tables, dev environment
  python -m generator upload

  # Specific tables only
  python -m generator upload --tables KNA1 VBAK VBAP

  # Production catalog
  python -m generator upload --env prod

  # Preview what would be uploaded, without touching Databricks
  python -m generator upload --dry-run

  # Custom local output dir
  python -m generator upload --out ./my_data
"""

import argparse
import subprocess
import sys
from pathlib import Path

TABLES = ["KNA1", "MARA", "MAKT", "VBAK", "VBAP", "BKPF", "BSEG", "TCURR"]
_LANDING = "/Volumes/sap_lab_{env}/landing/sap_raw"


def _databricks_mkdir(remote_dir: str, dry_run: bool) -> bool:
    # Creates intermediate directories in the volume before uploading.
    # Required on Windows: without this, 'fs cp' may fail for paths like
    # /Volumes/.../TABLE/batch=1/file.parquet when batch=1 doesn't exist yet.
    cmd = ["databricks", "fs", "mkdir", remote_dir]
    if dry_run:
        return True
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.returncode == 0  # non-fatal: directory may already exist


def _databricks_cp(local: Path, remote: str, dry_run: bool) -> bool:
    # str(local.resolve()) gives an absolute Windows path (e.g. C:\Users\...\out\MARA\...)
    # which the CLI unambiguously treats as a local file, avoiding confusion with the
    # remote /Volumes/... path that starts with a forward slash.
    local_abs = str(local.resolve())
    cmd = ["databricks", "fs", "cp", "--overwrite", local_abs, remote]
    if dry_run:
        print(f"    [dry-run] databricks fs cp --overwrite {local_abs} {remote}")
        return True
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"    ERROR: {result.stderr.strip()}", file=sys.stderr)
        return False
    return True


def _upload_table(table: str, out_dir: Path, landing_base: str, dry_run: bool) -> tuple[int, int]:
    table_dir = out_dir / table
    if not table_dir.exists():
        print(f"  [skip] {table_dir} does not exist — run 'python -m generator full' first")
        return 0, 0

    parquet_files = sorted(table_dir.rglob("*.parquet"))
    if not parquet_files:
        print(f"  [skip] no Parquet files found under {table_dir}")
        return 0, 0

    ok = err = 0
    seen_dirs: set[str] = set()
    for local_file in parquet_files:
        # Preserve relative path so batch=N subdirs are kept intact on the volume.
        rel = local_file.relative_to(table_dir).as_posix()
        remote = f"{landing_base}/{table}/{rel}"
        remote_dir = remote.rsplit("/", 1)[0]

        # Ensure the parent directory exists in the volume before uploading.
        # 'databricks fs cp' on Windows can fail for deep paths if the parent
        # directory was not yet created (batch=N dirs, for example).
        if remote_dir not in seen_dirs:
            _databricks_mkdir(remote_dir, dry_run)
            seen_dirs.add(remote_dir)

        print(f"  {local_file.relative_to(out_dir)}  →  {remote}")
        if _databricks_cp(local_file, remote, dry_run):
            ok += 1
        else:
            err += 1

    return ok, err


def cmd_upload(args: argparse.Namespace) -> None:
    out_dir = Path(args.out)
    landing_base = _LANDING.format(env=args.env)
    tables = args.tables

    print(f"Source : {out_dir.resolve()}")
    print(f"Target : {landing_base}")
    print(f"Tables : {', '.join(tables)}")
    if args.dry_run:
        print("Mode   : DRY-RUN (nothing will be uploaded)\n")
    else:
        print()

    total_ok = total_err = 0
    for table in tables:
        print(f"\n[{table}]")
        ok, err = _upload_table(table, out_dir, landing_base, args.dry_run)
        total_ok += ok
        total_err += err
        if ok:
            status = "dry-run OK" if args.dry_run else f"{ok} file(s) uploaded"
            print(f"  → {status}" + (f", {err} error(s)" if err else ""))

    print(f"\n{'─' * 50}")
    print(f"Total: {total_ok} files {'would be ' if args.dry_run else ''}uploaded, {total_err} errors.")

    if total_err:
        sys.exit(1)


def add_subparser(subparsers) -> None:
    p = subparsers.add_parser(
        "upload",
        help="Upload generated Parquet files to the Databricks landing volume",
    )
    p.add_argument(
        "--env",
        default="dev",
        choices=["dev", "prod"],
        help="Target environment (default: dev)",
    )
    p.add_argument(
        "--out",
        default="./out",
        metavar="DIR",
        help="Local output directory from the generator (default: ./out)",
    )
    p.add_argument(
        "--tables",
        nargs="+",
        choices=TABLES,
        default=TABLES,
        metavar="TABLE",
        help="Tables to upload (default: all). E.g.: --tables KNA1 VBAK VBAP",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the databricks fs cp commands without executing them",
    )
    p.set_defaults(func=cmd_upload)
