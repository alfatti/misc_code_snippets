import csv
import pandas as pd

def load_csv_force_width(path, expected_cols=106, merge_into=None, encoding_try=('utf-8', 'latin1')):
    """
    Read a messy CSV with NO row skipping and force exactly `expected_cols` columns.
    If a row has > expected_cols fields, the overflow is merged into `merge_into` column
    (by name) if provided, otherwise into the last column.
    """
    last_err = None
    for enc in encoding_try:
        try:
            with open(path, 'r', encoding=enc, errors='replace', newline='') as f:
                rdr = csv.reader(
                    f,
                    delimiter=',',
                    quotechar='"',
                    escapechar='\\',
                    doublequote=True,
                    strict=False
                )
                rows = list(rdr)
            break
        except Exception as e:
            last_err = e
    else:
        raise RuntimeError(f"Could not read file with encodings {encoding_try}: {last_err}")

    if not rows:
        return pd.DataFrame()

    header = rows[0]
    body = rows[1:]

    # If header has more than expected, trim; if fewer, pad with placeholders
    if len(header) < expected_cols:
        header = header + [f'__placeholder_{i}' for i in range(expected_cols - len(header))]
    elif len(header) > expected_cols:
        header = header[:expected_cols]

    # Figure out merge target index
    if merge_into and merge_into in header:
        target_idx = header.index(merge_into)
    else:
        target_idx = expected_cols - 1  # default: last column

    fixed_rows = []
    long_rows = 0
    short_rows = 0

    for line_no, r in enumerate(body, start=2):  # 1-based lines; +1 for header
        if len(r) == expected_cols:
            fixed_rows.append(r)
            continue

        if len(r) > expected_cols:
            # Merge overflow fields into target column (comma-join), keep everything
            overflow = r[expected_cols-1:]  # fields beyond the expected width
            kept = r[:expected_cols]        # first expected_cols fields
            if target_idx < expected_cols - 1:
                # move content currently at target to its place, and merge overflow there
                kept[target_idx] = (kept[target_idx] + ',' + ','.join(overflow)).rstrip(',')
            else:
                # target is the last col (default)
                kept[-1] = (kept[-1] + ',' + ','.join(overflow)).rstrip(',')
            fixed_rows.append(kept)
            long_rows += 1
        else:
            # Pad short rows
            kept = r + [''] * (expected_cols - len(r))
            fixed_rows.append(kept)
            short_rows += 1

    df = pd.DataFrame(fixed_rows, columns=header)

    print(f"Loaded {len(df)} rows with exactly {expected_cols} columns.")
    if long_rows:
        print(f"Rows that had >{expected_cols} fields (merged, nothing dropped): {long_rows}")
    if short_rows:
        print(f"Rows that had <{expected_cols} fields (padded): {short_rows}")

    return df

# ---- Usage ----
# If you know the free-text column that tends to overflow, put its exact header name in merge_into.
# Otherwise we merge into the LAST column.
df = load_csv_force_width(file_path, expected_cols=106, merge_into=None)

print(df.shape)
print(df.columns[:10])
