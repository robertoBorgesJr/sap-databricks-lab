import argparse
import json
import pathlib
import random

import pyarrow as pa
import pyarrow.parquet as pq

from generator.upload import add_subparser as add_upload_subparser
from generator.cdc import (
    generate_batch, apply_batch, load_state, save_state, write_batch,
    _SCHEMA as CDC_SCHEMA,
)
from generator.tables.bkpf import SCHEMA as BKPF_SCHEMA, generate as generate_bkpf
from generator.tables.bseg import SCHEMA as BSEG_SCHEMA, generate as generate_bseg
from generator.tables.kna1 import SCHEMA as KNA1_SCHEMA, generate as generate_kna1
from generator.tables.makt import SCHEMA as MAKT_SCHEMA, generate as generate_makt
from generator.tables.mara import SCHEMA as MARA_SCHEMA, generate as generate_mara
from generator.tables.tcurr import SCHEMA as TCURR_SCHEMA, generate as generate_tcurr
from generator.tables.vbak import SCHEMA as VBAK_SCHEMA, generate as generate_vbak
from generator.tables.vbap import SCHEMA as VBAP_SCHEMA, generate as generate_vbap


def _write_table(
    records: list[dict],
    schema_cols: list[str],
    dest: pathlib.Path,
    fmt: str,
    stem: str = "part-00000",
) -> pathlib.Path:
    dest.mkdir(parents=True, exist_ok=True)
    table = pa.table({col: [r[col] for r in records] for col in schema_cols})
    if fmt == "csv":
        import pyarrow.csv as csv_writer
        out_path = dest / f"{stem}.csv"
        csv_writer.write_csv(table, out_path)
    else:
        out_path = dest / f"{stem}.parquet"
        pq.write_table(table, out_path)
    return out_path


def _save_state(
    out: pathlib.Path,
    tables: dict[str, list[dict]],
    last_seq: int,
    last_batch: int,
) -> None:
    """Persist the current dataset state for the CDC simulator."""
    state_dir = out / "_state"
    state_dir.mkdir(parents=True, exist_ok=True)

    for table_name, records in tables.items():
        if not records:
            continue
        schema_map = {
            "KNA1": KNA1_SCHEMA, "MARA": MARA_SCHEMA, "MAKT": MAKT_SCHEMA,
            "VBAK": VBAK_SCHEMA, "VBAP": VBAP_SCHEMA,
            "BKPF": BKPF_SCHEMA, "BSEG": BSEG_SCHEMA, "TCURR": TCURR_SCHEMA,
        }
        cols = schema_map[table_name]
        table = pa.table({col: [r[col] for r in records] for col in cols})
        pq.write_table(table, state_dir / f"{table_name}.parquet")

    meta = {"last_seq": last_seq, "last_batch": last_batch}
    (state_dir / "meta.json").write_text(json.dumps(meta, indent=2))


def cmd_full(args: argparse.Namespace) -> None:
    rng = random.Random(args.seed)
    out = pathlib.Path(args.out)
    fmt = args.format

    steps = [
        ("KNA1",  args.n_customers,  "customers"),
        ("MARA",  args.n_materials,  "materials"),
        ("VBAK",  args.n_orders,     "sales orders"),
        ("BKPF",  args.n_docs,       "accounting docs"),
    ]
    for name, n, label in steps:
        print(f"  {name}: {n} {label} (seed={args.seed})")

    # --- masters (no foreign-key dependencies) ---
    print("\nGenerating masters ...")
    kna1 = generate_kna1(args.n_customers, rng)
    mara = generate_mara(args.n_materials, rng)
    kunnrs = [r["KUNNR"] for r in kna1]
    matnrs = [r["MATNR"] for r in mara]

    # --- dependent masters ---
    makt = generate_makt(len(matnrs), rng, matnrs)

    # --- transactional ---
    print("Generating transactional data ...")
    vbak = generate_vbak(args.n_orders, rng, kunnrs)
    vbap = generate_vbap(rng, vbak, matnrs)
    bkpf = generate_bkpf(args.n_docs, rng)
    bseg = generate_bseg(rng, bkpf)

    # --- reference ---
    print("Generating reference data ...")
    tcurr = generate_tcurr(rng)

    # --- write files ---
    print("\nWriting files ...")
    pairs = [
        ("KNA1", KNA1_SCHEMA, kna1),
        ("MARA", MARA_SCHEMA, mara),
        ("MAKT", MAKT_SCHEMA, makt),
        ("VBAK", VBAK_SCHEMA, vbak),
        ("VBAP", VBAP_SCHEMA, vbap),
        ("BKPF", BKPF_SCHEMA, bkpf),
        ("BSEG", BSEG_SCHEMA, bseg),
        ("TCURR", TCURR_SCHEMA, tcurr),
    ]
    for table_name, schema_cols, records in pairs:
        path = _write_table(records, schema_cols, out / table_name, fmt)
        print(f"  -> {path}  ({len(records)} rows)")

    # --- persist state for CDC ---
    state_tables = {
        "KNA1": kna1, "MARA": mara, "MAKT": makt,
        "VBAK": vbak, "VBAP": vbap,
        "BKPF": bkpf, "BSEG": bseg, "TCURR": tcurr,
    }
    _save_state(out, state_tables, last_seq=0, last_batch=0)
    print(f"\nState saved to {out / '_state'}")


def cmd_delta(args: argparse.Namespace) -> None:
    out = pathlib.Path(args.out)
    state = load_state(out)

    insert_rate, update_rate, delete_rate = (float(x) / 100 for x in args.mix.split(":"))
    rng = random.Random(args.seed + args.batch)

    print(f"Generating delta batch={args.batch} (seed={args.seed + args.batch}) ...")

    for table_name in CDC_SCHEMA:
        batch = generate_batch(
            table_name, state, args.batch, rng,
            insert_rate=insert_rate,
            update_rate=update_rate,
            delete_rate=delete_rate,
        )
        apply_batch(table_name, state, batch)
        path = write_batch(batch, table_name, out, args.batch, args.duplicate_file)
        print(f"  -> {path}  ({len(batch)} records)")

    state.last_batch = args.batch
    save_state(state, out)
    print(f"\nState updated. last_seq={state.last_seq}, last_batch={state.last_batch}")


def main() -> None:
    parser = argparse.ArgumentParser(prog="generator")
    sub = parser.add_subparsers(dest="command", required=True)

    full_p = sub.add_parser("full", help="Generate full initial load for all tables")
    full_p.add_argument("--seed", type=int, default=42)
    full_p.add_argument("--out", default="./out")
    full_p.add_argument("--n-customers", type=int, default=200, dest="n_customers")
    full_p.add_argument("--n-materials", type=int, default=150, dest="n_materials")
    full_p.add_argument("--n-orders",    type=int, default=300, dest="n_orders")
    full_p.add_argument("--n-docs",      type=int, default=100, dest="n_docs")
    full_p.add_argument("--format", choices=["parquet", "csv"], default="parquet")
    full_p.set_defaults(func=cmd_full)

    delta_p = sub.add_parser("delta", help="Generate a CDC delta batch")
    delta_p.add_argument("--batch", type=int, required=True, help="Batch number (e.g. 1, 2, 3)")
    delta_p.add_argument("--seed",  type=int, default=42)
    delta_p.add_argument("--out",   default="./out")
    delta_p.add_argument(
        "--mix", default="10:20:5",
        metavar="INSERT:UPDATE:DELETE",
        help="Mix as integer percentages, e.g. '10:20:5'"
    )
    delta_p.add_argument(
        "--duplicate-file", action="store_true", dest="duplicate_file",
        help="Also write part-00001.parquet (same content) to simulate redelivery"
    )
    delta_p.set_defaults(func=cmd_delta)

    add_upload_subparser(sub)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
