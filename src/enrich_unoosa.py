from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

from .crosswalk import build_satcat_crosswalk, merge_unoosa_with_crosswalk
from .n2yo_client import N2YOClient
from .tle_parse import parse_tle_fields


def enrich_with_n2yo(
    unoosa_df: pd.DataFrame,
    satcat_csv_path: str | Path,
    cache_dir: str | Path = "cache_tle",
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """End-to-end enrichment using N2YO TLE + derived features.

    Returns (enriched_df, tle_df, tle_feats_df).
    """
    # Build crosswalk and merge to get NORAD IDs
    satcat_df = pd.read_csv(satcat_csv_path)
    satcat_xwalk = build_satcat_crosswalk(satcat_df)
    merged = merge_unoosa_with_crosswalk(unoosa_df, satcat_xwalk)

    # Fetch TLEs for unique NORAD IDs
    unique_ids = (
        merged["norad_id"].dropna().astype("Int64").drop_duplicates().astype(int).tolist()
    )

    client = N2YOClient(cache_dir=cache_dir)
    rows: List[Dict] = []
    for nid in unique_ids:
        try:
            payload = client.get_tle(int(nid))
            rows.append(
                {
                    "norad_id": int(nid),
                    "n2yo_satname": (payload.get("info", {}) or {}).get("satname"),
                    "n2yo_txn_last_60min": (payload.get("info", {}) or {}).get(
                        "transactionscount"
                    ),
                    "tle_one_line": payload.get("tle"),
                }
            )
        except Exception as exc:
            rows.append({"norad_id": int(nid), "error": str(exc)})

    tle_df = pd.DataFrame(rows)

    # Parse TLEs into features
    feat_rows: List[Dict] = []
    for _, r in tle_df.iterrows():
        feats = parse_tle_fields(r.get("tle_one_line"))
        feats["norad_id"] = r["norad_id"]
        feat_rows.append(feats)
    tle_feats_df = pd.DataFrame(feat_rows)

    # Merge back
    enriched = (
        merged.merge(
            tle_df[["norad_id", "n2yo_satname", "n2yo_txn_last_60min", "tle_one_line"]],
            on="norad_id",
            how="left",
        ).merge(tle_feats_df, on="norad_id", how="left")
    )

    # Freshness
    if "tle_epoch" in enriched.columns:
        enriched["tle_age_days"] = (
            pd.Timestamp.utcnow() - pd.to_datetime(enriched["tle_epoch"], utc=True)
        ).dt.total_seconds() / 86400.0

    return enriched, tle_df, tle_feats_df


__all__ = ["enrich_with_n2yo"]


