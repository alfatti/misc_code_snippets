import pandas as pd
import numpy as np

def clean_trade_date_and_features(
    df: pd.DataFrame,
    col: str = "trade_dat",
    *,
    min_valid_date: str = "1990-01-01",
    overwrite: bool = True,
    out_col: str | None = None
) -> pd.DataFrame:
    """
    Clean a mixed-format trade date column and add year/month features.

    Steps
    -----
    1) Normalize null-like tokens: {'0','0.0','na','n/a','nan','none',''}
    2) Parse dates from either YYYYMMDD or general datetime-like strings
    3) Invalidate 'epoch defaults' or absurdly old dates (before min_valid_date)
    4) Add: <out_col> (datetime64[ns]), <out_col>_year, <out_col>_month,
            <out_col>_yyyymm (string 'YYYYMM'), <out_col>_period (Period[M]),
            <out_col>_is_missing (bool)

    Parameters
    ----------
    df : DataFrame
    col : str
        Name of the input column to clean (e.g., 'trade_dat').
    min_valid_date : str
        Dates earlier than this are set to NaT (default '1990-01-01').
    overwrite : bool
        If True, cleaned dates overwrite `col`. If False, create a new column.
    out_col : str | None
        Name for the cleaned date column (ignored if overwrite=True).
        If overwrite=False and out_col is None, uses f"{col}_dt".

    Returns
    -------
    DataFrame
        The input df with cleaned date column and feature columns added.
    """
    s = df[col].astype(str).str.strip()

    # 1) null-like normalization (vectorized)
    invalid_vals = {"0", "0.0", "na", "n/a", "nan", "none", ""}
    s = s.mask(s.str.lower().isin(invalid_vals))

    # 2) parse dates
    # Try fast path for 8-digit YYYYMMDD
    is_yyyymmdd = s.str.len().eq(8) & s.str.isdigit()
    parsed_fast = pd.to_datetime(s.where(is_yyyymmdd), format="%Y%m%d", errors="coerce")

    # Fallback for everything else
    parsed_fallback = pd.to_datetime(s.where(~is_yyyymmdd), errors="coerce", utc=False)

    cleaned = parsed_fast.combine_first(parsed_fallback)

    # 3) invalidate absurdly old / epoch-default dates
    cutoff = pd.Timestamp(min_valid_date)
    cleaned = cleaned.mask(cleaned < cutoff)

    # Decide destination column
    dest_col = col if overwrite else (out_col or f"{col}_dt")
    df[dest_col] = cleaned

    # 4) add features
    df[f"{dest_col}_is_missing"] = df[dest_col].isna()
    df[f"{dest_col}_year"] = df[dest_col].dt.year.astype("Int64")
    df[f"{dest_col}_month"] = df[dest_col].dt.month.astype("Int64")
    # String 'YYYYMM' keeps NaT as NaN; if you prefer Int64, uncomment the two lines below
    df[f"{dest_col}_yyyymm"] = df[dest_col].dt.strftime("%Y%m")
    df[f"{dest_col}_period"] = df[dest_col].dt.to_period("M")

    return df

# ---- Example usage ----
# df = clean_trade_date_and_features(df, col="trade_dat", min_valid_date="1990-01-01", overwrite=True)
# Now you have:
#   - df['trade_dat'] as datetime64[ns] (cleaned)
#   - df['trade_dat_year'], df['trade_dat_month']
#   - df['trade_dat_yyyymm'] (e.g., '202504'), df['trade_dat_period'] (Period[M])
#   - df['trade_dat_is_missing'] as a quick flag
