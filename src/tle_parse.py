from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Any

import numpy as np
from sgp4.api import Satrec


def parse_tle_fields(tle_one_line: str) -> Dict[str, Any]:
    """Parse a joined two-line TLE string into derived orbit features.

    Returns empty dict on malformed input.
    """
    if not tle_one_line or not isinstance(tle_one_line, str):
        return {}

    lines = [ln.strip() for ln in tle_one_line.splitlines() if ln.strip()]
    if len(lines) < 2:
        return {}

    l1, l2 = lines[0], lines[1]
    try:
        sat = Satrec.twoline2rv(l1, l2)
    except Exception:
        return {}

    jd = sat.jdsatepoch + sat.jdsatepochF
    epoch_unix = (jd - 2440587.5) * 86400.0
    tle_epoch_dt = datetime.fromtimestamp(epoch_unix, tz=timezone.utc)

    # Mean motion in rev/day
    try:
        mean_motion_rev_per_day = (sat.no_kozai * 1440.0) / (2.0 * np.pi)
    except Exception:
        mean_motion_rev_per_day = np.nan

    # Semi-major axis from mean motion
    mu_earth = 398600.4418  # km^3/s^2
    try:
        n_rad_s = (mean_motion_rev_per_day * 2.0 * np.pi) / 86400.0
        semi_major_axis_km = (mu_earth ** (1.0 / 3.0)) / (n_rad_s ** (2.0 / 3.0))
    except Exception:
        semi_major_axis_km = np.nan

    ecc = getattr(sat, "ecco", np.nan)
    try:
        perigee_km = semi_major_axis_km * (1.0 - ecc) - 6378.137
        apogee_km = semi_major_axis_km * (1.0 + ecc) - 6378.137
    except Exception:
        perigee_km = np.nan
        apogee_km = np.nan

    out = {
        "tle_epoch": tle_epoch_dt.isoformat(),
        "inclination_deg": float(np.degrees(getattr(sat, "inclo", np.nan))),
        "raan_deg": float(np.degrees(getattr(sat, "nodeo", np.nan))),
        "argp_deg": float(np.degrees(getattr(sat, "argpo", np.nan))),
        "mean_anomaly_deg": float(np.degrees(getattr(sat, "mo", np.nan))),
        "eccentricity": float(ecc) if not isinstance(ecc, float) else ecc,
        "mean_motion_rev_per_day": float(mean_motion_rev_per_day),
        "period_min": float(1440.0 / mean_motion_rev_per_day)
        if mean_motion_rev_per_day and not np.isnan(mean_motion_rev_per_day)
        else np.nan,
        "semi_major_axis_km": float(semi_major_axis_km),
        "perigee_km": float(perigee_km),
        "apogee_km": float(apogee_km),
    }
    return out


__all__ = ["parse_tle_fields"]


