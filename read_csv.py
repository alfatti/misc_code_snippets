import csv, re, io
import pandas as pd
from collections import Counter

def _detect_delimiter(sample_lines, candidates=(',', ';', '|', '\t')):
    scores = {}
    for d in candidates:
        counts = [line.count(d) for line in sample_lines if line.strip()]
        if not counts:
            continue
        # Score: mode count * consistency (inverse variance)
        mode = Counter(counts).most_common(1)[0][0]
        variance = sum((c - mode)**2 for c in counts) / max(len(counts), 1)
        scores[d] = (mode, -variance)   # higher mode, lower variance
    if not scores:
        return None
    # pick delimiter with highest (mode, -variance)
    return sorted(scores.items(), key=lambda kv: (kv[1][0], kv[1][1]), reverse=True)[0][0]

def _read_head(path, n=2000, encoding='utf-8'):
    with open(path, 'r', encoding=encoding, errors='replace') as f:
        return [f.readline() for _ in range(50)], f.read(n)

def robust_read_csv(path, preferred_encoding='utf-8'):
    # 0) Read a small sample
    head_lines, remainder = _read_head(path, encoding=preferred_encoding)
    sample = ''.join(head_lines)

    # 1) Delimiter guess (fallback to comma)
    delim = _detect_delimiter(head_lines) or ','

    # 2) Quick sanity on column counts in the head
    counts = [ln.count(delim) for ln in head_lines if ln.strip()]
    mode_cols = Counter(counts).most_common(1)[0][0] if counts else None

    # 3) Try straightforward parse (normal quoting)
    try:
        df = pd.read_csv(path, sep=delim, engine='python', dtype=str)
        return df
    except Exception as e1:
        first_err = e1

    # 4) Try with explicit quote/escape (no line skips)
    try:
        df = pd.read_csv(
            path, sep=delim, engine='python', dtype=str,
            quotechar='"', doublequote=True, escapechar='\\'
        )
        return df
    except Exception as e2:
        second_err = e2

    # 5) Regex split: delimiter only outside quotes
    #    Works when some rows have embedded delimiters within balanced quotes.
    regex_sep = {
        ',': r',(?=(?:[^"]*"[^"]*")*[^"]*$)',
        ';': r';(?=(?:[^"]*"[^"]*")*[^"]*$)',
        '|': r'\|(?=(?:[^"]*"[^"]*")*[^"]*$)',
        '\t': r'\t(?=(?:[^"]*"[^"]*")*[^"]*$)'
    }.get(delim, None)

    if regex_sep is not None:
        try:
            df = pd.read_csv(
                path, sep=regex_sep, engine='python', dtype=str, header='infer'
            )
            return df
        except Exception as e3:
            third_err = e3
    else:
        third_err = None

    # 6) If quoting is *broken* (unbalanced quotes), normalize quotes in-memory then parse.
    #    We replace smart quotes and ensure even quote counts per line (soft fix).
    def normalize_text(text):
        # unify smart quotes -> "
        text = (text
                .replace('“', '"').replace('”', '"')
                .replace('‘', "'").replace('’', "'"))
        # best-effort: if a line has odd count of ", replace lone " inside fields with ''
        fixed_lines = []
        for ln in text.splitlines(keepends=True):
            if ln.count('"') % 2 == 1:
                # replace stray " with ''
                fixed_lines.append(ln.replace('"', "''"))
            else:
                fixed_lines.append(ln)
        return ''.join(fixed_lines)

    try:
        with open(path, 'r', encoding=preferred_encoding, errors='replace') as f:
            raw = f.read()
        fixed = normalize_text(raw)

        # Try regex-aware parse on fixed text (no line skipping)
        if regex_sep is None:
            # If we couldn't build a regex for the delimiter, fall back to detected delim.
            regex_sep = re.escape(delim) + r'(?=(?:[^"]*"[^"]*")*[^"]*$)'

        df = pd.read_csv(
            io.StringIO(fixed),
            sep=regex_sep,
            engine='python',
            dtype=str
        )
        return df
    except Exception as e4:
        # Final fallback: show concise diagnostics
        msg = [
            f"Could not parse CSV without skipping lines.",
            f"Delimiter guess: {repr(delim)}; head mode delimiter count: {mode_cols}",
            f"Errors:",
            f" - plain read_csv: {type(first_err).__name__}: {first_err}",
            f" - quote/escape:   {type(second_err).__name__}: {second_err}",
            f" - regex outside quotes: {type(third_err).__name__ if third_err else 'n/a'}: {third_err if third_err else ''}",
            f" - normalized text parse: {type(e4).__name__}: {e4}",
        ]
        raise RuntimeError("\n".join(msg)) from e4
