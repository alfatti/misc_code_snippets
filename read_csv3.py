import csv, io
from collections import Counter
import pandas as pd

def _looks_utf16(raw: bytes) -> bool:
    # Heuristic: UTF-16 often has many NULs (every other byte)
    if raw.startswith(b'\xff\xfe') or raw.startswith(b'\xfe\xff'):
        return True  # BOM
    if len(raw) >= 200:
        sample = raw[:200]
        zero_ratio = sample.count(b'\x00') / len(sample)
        return zero_ratio > 0.1
    return False

def _decode_text_safely(path, encodings=('utf-8-sig','utf-8','cp1252','latin1')):
    """Read bytes, auto-handle UTF-16/NULs, return clean str."""
    with open(path, 'rb') as f:
        raw = f.read()

    # UTF-16? Decode with BOM-aware codec (handles LE/BE)
    if _looks_utf16(raw):
        try:
            text = raw.decode('utf-16')  # BOM-aware if present
            return text.replace('\x00', '')
        except Exception:
            pass  # fall through to other attempts

    # Try common encodings
    last_err = None
    for enc in encodings:
        try:
            text = raw.decode(enc, errors='replace')
            # Remove NULs if any remain
            return text.replace('\x00', '')
        except Exception as e:
            last_err = e
    raise RuntimeError(f"Could not decode file with encodings {encodings}: {last_err}")

def _detect_delim_from_text(text, candidates=(',', ';', '|', '\t')):
    lines = [ln for ln in text.splitlines()[:200] if ln.strip()]
    scores = {}
    for d in candidates:
        counts = [ln.count(d) for ln in lines]
        if not counts:
            continue
        mode = Counter(counts).most_common(1)[0][0]
        var = sum((c - mode)**2 for c in counts) / max(len(counts), 1)
        scores[d] = (mode, -var)
    if not scores:
        return ','
    return sorted(scores.items(), key=lambda kv: (kv[1][0], kv[1][1]), reverse=True)[0][0]

def load_csv_force_width_resilient(
    path,
    expected_cols=106,
    merge_into=None,
    delimiter=None,
):
    """
    Robust loader:
      - Handles UTF-16 / NUL bytes
      - No skipped rows
      - Forces exactly `expected_cols` columns
      - Merges overflow fields into `merge_into` (or last column)
    """
    # 1) Get clean text (no NULs), auto-handle UTF-16
    text = _decode_text_safely(path)

    # 2) Choose delimiter (auto if not provided)
    delim = delimiter or _detect_delim_from_text(text)

    # 3) CSV-parse from memory (string), then normalize widths
    sio = io.StringIO(text)
    rdr = csv.reader(
        sio,
        delimiter=delim,
        quotechar='"',
        escapechar='\\',
        doublequote=True,
        strict=False
    )
    rows = list(rdr)
    if not rows:
        return pd.DataFrame()

    header = rows[0]
    body = rows[1:]

    # Normalize header length to expected width
    if len(header) < expected_cols:
        header = header + [f'__placeholder_{i}' for i in range(expected_cols - len(header))]
    elif len(header) > expected_cols:
        header = header[:expected_cols]

    # Merge target column index
    target_idx = header.index(merge_into) if (merge_into and merge_into in header) else expected_cols - 1

    fixed_rows, long_rows, short_rows = [], 0, 0
    for r in body:
        if len(r) == expected_cols:
            fixed_rows.append(r)
        elif len(r) > expected_cols:
            overflow = r[expected_cols-1:]
            kept = r[:expected_cols]
            if target_idx < expected_cols - 1:
                kept[target_idx] = (kept[target_idx] + ',' + ','.join(overflow)).rstrip(',')
            else:
                kept[-1] = (kept[-1] + ',' + ','.join(overflow)).rstrip(',')
            fixed_rows.append(kept)
            long_rows += 1
        else:
            kept = r + [''] * (expected_cols - len(r))
            fixed_rows.append(kept)
            short_rows += 1

    df = pd.DataFrame(fixed_rows, columns=header)

    print(f"Loaded {len(df)} rows with exactly {expected_cols} columns. Delimiter='{delim}'")
    if long_rows:
        print(f"Rows with >{expected_cols} fields (merged, nothing dropped): {long_rows}")
    if short_rows:
        print(f"Rows with <{expected_cols} fields (padded): {short_rows}")
    return df
