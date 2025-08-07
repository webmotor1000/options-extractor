#!/usr/bin/env python3
"""
Extract and filter option rows from a large TXT/TSV based on the user's schema.

Filters:
- QUOTE_READTIME time must be 09:30 or 15:45 (regardless of date).
- DTE between 0 and 14 inclusive.
Selection:
- For each (QUOTE_READTIME, EXPIRE_DATE), keep the 10 strikes closest below
  and the 10 closest above UNDERLYING_LAST (by absolute distance).

Input columns (with or without brackets):
[QUOTE_UNIXTIME] [QUOTE_READTIME] [QUOTE_DATE] [QUOTE_TIME_HOURS]
[UNDERLYING_LAST] [EXPIRE_DATE] [EXPIRE_UNIX] [DTE]
[C_DELTA] [C_GAMMA] [C_VEGA] [C_THETA] [C_RHO] [C_IV] [C_VOLUME] [C_LAST]
[C_SIZE] [C_BID] [C_ASK] [STRIKE] [P_BID] [P_ASK] [P_SIZE] [P_LAST]
[P_DELTA] [P_GAMMA] [P_VEGA] [P_THETA] [P_RHO] [P_IV] [P_VOLUME]
[STRIKE_DISTANCE] [STRIKE_DISTANCE_PCT]

Notes:
- SIZE columns stay as text (e.g., "1 x 1").
- If STRIKE_DISTANCE missing, compute as STRIKE - UNDERLYING_LAST.
- If STRIKE_DISTANCE_PCT missing, compute as (STRIKE - UNDERLYING_LAST)/UNDERLYING_LAST.

Usage (local): python extract_options.py --in /path/file.txt --out output.csv
"""

import argparse
import sys
import pandas as pd
import numpy as np
from pathlib import Path

TIME_WHITELIST = {"09:30", "15:45"}

NUMERIC = {
    "QUOTE_UNIXTIME": "Int64",
    "QUOTE_TIME_HOURS": "float64",
    "UNDERLYING_LAST": "float64",
    "EXPIRE_UNIX": "Int64",
    "DTE": "Int64",
    "C_DELTA": "float64", "C_GAMMA": "float64", "C_VEGA": "float64",
    "C_THETA": "float64", "C_RHO": "float64", "C_IV": "float64",
    "C_VOLUME": "Int64", "C_LAST": "float64",
    "C_BID": "float64", "C_ASK": "float64",
    "STRIKE": "float64",
    "P_BID": "float64", "P_ASK": "float64", "P_LAST": "float64",
    "P_DELTA": "float64", "P_GAMMA": "float64", "P_VEGA": "float64",
    "P_THETA": "float64", "P_RHO": "float64", "P_IV": "float64",
    "P_VOLUME": "Int64",
    "STRIKE_DISTANCE": "float64", "STRIKE_DISTANCE_PCT": "float64",
}

OUTPUT_ORDER = [
    "QUOTE_READTIME","UNDERLYING_LAST","EXPIRE_DATE","DTE",
    "C_THETA","C_IV","C_VOLUME","C_LAST","C_SIZE",
    "C_BID","C_ASK","STRIKE","P_BID","P_ASK","P_SIZE","P_LAST",
    "P_THETA","P_IV","P_VOLUME","STRIKE_DISTANCE","STRIKE_DISTANCE_PCT"
]

def normalize_headers(cols):
    out = []
    for c in cols:
        c2 = str(c).strip()
        if c2.startswith("[") and c2.endswith("]"):
            c2 = c2[1:-1].strip()
        out.append(c2)
    return out

def time_hhmm(s):
    if pd.isna(s): return None
    t = str(s).strip()
    if not t: return None
    time_token = t.split()[-1]
    if len(time_token) >= 5 and ":" in time_token:
        hhmm = time_token[:5]
        try:
            hh, mm = hhmm.split(":")
            if len(hh)==2 and len(mm)==2 and hh.isdigit() and mm.isdigit():
                return f"{hh}:{mm}"
        except Exception:
            return None
    return None

def ensure_numeric(df):
    for col, typ in NUMERIC.items():
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

def compute_distances(df):
    if "STRIKE" in df.columns and "UNDERLYING_LAST" in df.columns:
        if "STRIKE_DISTANCE" not in df.columns or df["STRIKE_DISTANCE"].isna().all():
            df["STRIKE_DISTANCE"] = df["STRIKE"] - df["UNDERLYING_LAST"]
        if "STRIKE_DISTANCE_PCT" not in df.columns or df["STRIKE_DISTANCE_PCT"].isna().all():
            with np.errstate(divide='ignore', invalid='ignore'):
                df["STRIKE_DISTANCE_PCT"] = (df["STRIKE"] - df["UNDERLYING_LAST"]) / df["UNDERLYING_LAST"]
    return df

def select_10_above_below(df):
    if df.empty: return df
    df = compute_distances(df.copy())
    above = df[df["STRIKE_DISTANCE"] >= 0].copy()
    below = df[df["STRIKE_DISTANCE"] < 0].copy()
    above["_abs"] = np.abs(above["STRIKE_DISTANCE"])
    below["_abs"] = np.abs(below["STRIKE_DISTANCE"])
    above = above.sort_values("_abs", kind="stable").head(10)
    below = below.sort_values("_abs", kind="stable").head(10)
    out = pd.concat([below, above], ignore_index=True)
    return out.drop(columns=["_abs"], errors="ignore")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--out", dest="out", required=True)
    ap.add_argument("--sep", default="\t")
    ap.add_argument("--chunksize", type=int, default=250000)
    args = ap.parse_args()

    in_path = Path(args.inp)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    header_written = False

    # Probe header to normalize names
    sample = pd.read_csv(in_path, sep=args.sep, nrows=1, dtype=str, engine="python", on_bad_lines="skip")
    sample.columns = normalize_headers(sample.columns)
    usecols = list(set(sample.columns))  # read all available columns

    dtype_map = {}
    for col in usecols:
        dtype_map[col] = NUMERIC.get(col, "string")

    for chunk in pd.read_csv(in_path, sep=args.sep, dtype=dtype_map, usecols=usecols,
                             engine="python", on_bad_lines="skip", chunksize=args.chunksize):
        chunk.columns = normalize_headers(chunk.columns)
        if "DTE" in chunk.columns:
            chunk["DTE"] = pd.to_numeric(chunk["DTE"], errors="coerce")
            chunk = chunk[(chunk["DTE"] >= 0) & (chunk["DTE"] <= 14)]

        if "QUOTE_READTIME" in chunk.columns:
            hhmm = chunk["QUOTE_READTIME"].map(time_hhmm)
            mask = hhmm.isin(TIME_WHITELIST)
            chunk = chunk[mask].copy()
            chunk["HHMM"] = hhmm.loc[chunk.index]

        if chunk.empty:
            continue

        ensure_numeric(chunk)
        chunk = compute_distances(chunk)

        group_cols = [c for c in ["QUOTE_READTIME", "EXPIRE_DATE"] if c in chunk.columns]
        if group_cols:
            selected = (chunk.groupby(group_cols, dropna=False, sort=False, group_keys=False)
                             .apply(select_10_above_below))
        else:
            selected = select_10_above_below(chunk)

        if selected.empty:
            continue

        cols = [c for c in OUTPUT_ORDER if c in selected.columns]
        extras = [c for c in ["QUOTE_DATE","QUOTE_TIME_HOURS","EXPIRE_UNIX",
                              "C_DELTA","C_GAMMA","C_VEGA","C_RHO",
                              "P_DELTA","P_GAMMA","P_VEGA","P_RHO",
                              "QUOTE_UNIXTIME","P_VOLUME","C_VOLUME","HHMM"]
                  if c in selected.columns]
        remaining = [c for c in selected.columns if c not in set(cols + extras)]
        cols_out = cols + extras + remaining

        selected.to_csv(out_path, mode="a", header=(not header_written), index=False, columns=cols_out)
        header_written = True

if __name__ == "__main__":
    main()
