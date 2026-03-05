import json
from pathlib import Path
import math

REPORT = Path(r"D:\MyApps\Autobot\data\features\features_v3\_meta\build_report.json")

def walk(obj, path="$"):
    if isinstance(obj, dict):
        yield path, obj
        for k, v in obj.items():
            yield from walk(v, f"{path}.{k}")
    elif isinstance(obj, list):
        yield path, obj
        for i, v in enumerate(obj):
            yield from walk(v, f"{path}[{i}]")

def pick_key(d, candidates):
    for k in candidates:
        if k in d:
            return k
    return None

def find_per_market_tables(j):
    """Return list of (path, rows) where rows is list[dict] that look like per-market stats."""
    tables = []
    for path, obj in walk(j):
        if isinstance(obj, list) and obj and all(isinstance(x, dict) for x in obj):
            # Must contain market-like key
            mk = None
            for k in ("market", "code", "symbol"):
                if k in obj[0]:
                    mk = k
                    break
            if mk is None:
                continue
            tables.append((path, obj))
    return tables

def normalize_market_key(row):
    for k in ("market", "code", "symbol"):
        if k in row:
            return k
    return None

def to_float(x):
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    try:
        return float(x)
    except Exception:
        return None

def main():
    if not REPORT.exists():
        raise SystemExit(f"build_report not found: {REPORT}")

    j = json.loads(REPORT.read_text(encoding="utf-8"))

    tables = find_per_market_tables(j)

    # Collect candidates for dropped_one_m + synth_ratio from any per-market tables
    dropped_rows = {}   # market -> dropped_one_m
    synth_ratio = {}    # market -> synth_ratio_mean (or best available)
    extra = {}          # market -> dict of extra fields if available

    dropped_key_candidates = [
        "rows_dropped_one_m",
        "rows_dropped_one_m_after_label",
        "rows_dropped_one_m_window_missing",
        "rows_dropped_one_m_guard",
        "dropped_one_m",
        "rows_dropped_one_m_total",
    ]
    synth_key_candidates = [
        "one_m_synth_ratio_mean",
        "one_m_synth_ratio",
        "one_m_synth_ratio_avg",
        "one_m_synth_ratio_p50",  # fallback if mean missing
    ]
    # optional extras you might have
    extra_keys = [
        "rows_after_label",
        "rows_final",
        "one_m_synth_ratio_p50",
        "one_m_synth_ratio_p90",
        "one_m_real_count_mean",
        "one_m_real_volume_sum_mean",
        "one_m_real_count",
        "one_m_real_volume_sum",
    ]

    used_sources = {"dropped": set(), "synth": set()}

    for path, rows in tables:
        mk = normalize_market_key(rows[0])
        if mk is None:
            continue

        # Find dropped key in this table
        dk = None
        for cand in dropped_key_candidates:
            if cand in rows[0]:
                dk = cand
                break

        # Find synth key in this table
        sk = None
        for cand in synth_key_candidates:
            if cand in rows[0]:
                sk = cand
                break

        for r in rows:
            m = r.get(mk)
            if not m:
                continue

            if dk is not None:
                v = to_float(r.get(dk))
                if v is not None:
                    # keep max if multiple sources exist
                    prev = dropped_rows.get(m)
                    if prev is None or v > prev:
                        dropped_rows[m] = v
                        used_sources["dropped"].add(path + f" (key={dk})")

            if sk is not None:
                v = to_float(r.get(sk))
                if v is not None:
                    prev = synth_ratio.get(m)
                    # prefer mean over p50 if both appear; heuristic: keep first mean-like key
                    if prev is None:
                        synth_ratio[m] = v
                        used_sources["synth"].add(path + f" (key={sk})")

            # extras
            if m not in extra:
                extra[m] = {}
            for ek in extra_keys:
                if ek in r and ek not in extra[m]:
                    extra[m][ek] = r.get(ek)

    if not dropped_rows:
        print("ERROR: per-market dropped_one_m not found in build_report.json")
        print("HINT: search keys like 'rows_dropped_one_m' in the file.")
        return

    # Build Top10 by dropped_one_m desc
    top = sorted(dropped_rows.items(), key=lambda kv: kv[1], reverse=True)[:10]

    print("== build_report.json ==")
    print(str(REPORT))
    print()
    print("== Sources used ==")
    print("dropped:", "; ".join(sorted(used_sources["dropped"])) if used_sources["dropped"] else "(unknown)")
    print("synth  :", "; ".join(sorted(used_sources["synth"])) if used_sources["synth"] else "(not found)")
    print()

    print("== TOP10 markets by rows_dropped_one_m ==")
    header = [
        "rank","market","dropped_one_m",
        "one_m_synth_ratio","one_m_real_ratio",
        "synth_p50","synth_p90",
        "rows_after_label","rows_final",
        "one_m_real_count_mean","one_m_real_volume_sum_mean"
    ]
    print(",".join(header))

    for i,(m,dropv) in enumerate(top, start=1):
        sr = synth_ratio.get(m)
        rr = (1.0 - sr) if sr is not None else None
        ex = extra.get(m, {})
        row = [
            i,
            m,
            int(dropv) if abs(dropv-round(dropv))<1e-9 else dropv,
            f"{sr:.6f}" if sr is not None else "",
            f"{rr:.6f}" if rr is not None else "",
            f"{to_float(ex.get('one_m_synth_ratio_p50')):.6f}" if ex.get("one_m_synth_ratio_p50") is not None else "",
            f"{to_float(ex.get('one_m_synth_ratio_p90')):.6f}" if ex.get("one_m_synth_ratio_p90") is not None else "",
            ex.get("rows_after_label",""),
            ex.get("rows_final",""),
            ex.get("one_m_real_count_mean",""),
            ex.get("one_m_real_volume_sum_mean",""),
        ]
        print(",".join(map(str,row)))

    print()
    print("NOTE: one_m_real_ratio = 1 - one_m_synth_ratio")
    print("If synth ratio is empty, your report did not include per-market synth stats (or key name differs).")

if __name__ == "__main__":
    main()
