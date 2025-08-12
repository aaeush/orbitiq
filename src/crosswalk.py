from __future__ import annotations

from typing import Dict, Optional

import pandas as pd


def normalize_intldes(series: pd.Series) -> pd.Series:
    """Normalize international designators to uppercase with no spaces."""
    return (
        series.astype(str)
        .str.upper()
        .str.replace(r"\s+", "", regex=True)
        .str.strip()
    )


def build_satcat_crosswalk(satcat_df: pd.DataFrame) -> pd.DataFrame:
    """Return minimal crosswalk with normalized INTLDES and NORAD_CAT_ID."""
    df = satcat_df.copy()
    if "INTLDES" not in df.columns or "NORAD_CAT_ID" not in df.columns:
        raise ValueError("satcat_df must contain columns INTLDES and NORAD_CAT_ID")
    df["INTLDES"] = normalize_intldes(df["INTLDES"])  # type: ignore[index]
    df = df.dropna(subset=["INTLDES", "NORAD_CAT_ID"])  # type: ignore[list-item]
    df = df.rename(columns={"NORAD_CAT_ID": "norad_id"})
    # Some catalogs use numeric strings; ensure integers where possible
    df["norad_id"] = pd.to_numeric(df["norad_id"], errors="coerce").astype("Int64")
    return df[["INTLDES", "norad_id", "SATNAME", "COUNTRY", "LAUNCH", "DECAY"]].copy()


def merge_unoosa_with_crosswalk(
    unoosa_df: pd.DataFrame,
    satcat_crosswalk: pd.DataFrame,
    unoosa_intldes_col: str = "international_designator",
) -> pd.DataFrame:
    """Merge UNOOSA data with SATCAT crosswalk to obtain norad_id.

    Adds columns: intldes (normalized), norad_id, and SATCAT metadata when available.
    """
    if unoosa_intldes_col not in unoosa_df.columns:
        raise ValueError(f"UNOOSA dataframe lacks column: {unoosa_intldes_col}")

    df = unoosa_df.copy()
    df["intldes"] = normalize_intldes(df[unoosa_intldes_col])
    merged = df.merge(
        satcat_crosswalk,
        left_on="intldes",
        right_on="INTLDES",
        how="left",
    )
    return merged.drop(columns=["INTLDES"]).rename(columns={"SATNAME": "satcat_satname"})


__all__ = [
    "normalize_intldes",
    "build_satcat_crosswalk",
    "merge_unoosa_with_crosswalk",
]


