"""
Working calendar of Extremadura for the ML jobs: the same holidays as
gold_common.holidays (calendario of config_gold.json), without Spark.

- easter(year): Easter Sunday of the Gregorian calendar.
- holidays(years): national holidays of Spain, Maundy Thursday, Good Friday
  and the Día de Extremadura (8 September). The local holidays of every
  municipality are not included.
- season(month): 1 winter (December to February), 2 spring, 3 summer, 4
  autumn, as add_season of job_ml_train.
- working_days(timestamps): Monday to Friday and not a holiday.
"""

from datetime import date, timedelta

import numpy as np
import pandas as pd

FIXED_HOLIDAYS = [(1, 1), (1, 6), (5, 1), (8, 15), (9, 8), (10, 12), (11, 1), (12, 6), (12, 8), (12, 25)]


def easter(year: int) -> date:
    """Easter Sunday of the Gregorian calendar (anonymous algorithm)."""
    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    weekday = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * weekday) // 451
    month, day = divmod(h + weekday - 7 * m + 114, 31)
    return date(year, month, day + 1)


def holidays(years) -> set:
    """Holidays common to the whole of Extremadura: national, Maundy Thursday, Good Friday and 8 September."""
    out = set()

    for year in years:
        out |= {date(year, month, day) for month, day in FIXED_HOLIDAYS}
        sunday = easter(year)
        out |= {sunday - timedelta(days=3), sunday - timedelta(days=2)}

    return out


def season(month: pd.Series) -> pd.Series:
    """1 winter (December to February), 2 spring, 3 summer, 4 autumn."""
    return month.map(lambda m: 1.0 if m in (12, 1, 2) else 2.0 if m in (3, 4, 5) else 3.0 if m in (6, 7, 8) else 4.0)


def holiday_flags(timestamps: pd.Series) -> np.ndarray:
    """True for the timestamps that fall on a holiday."""
    stamps = pd.to_datetime(timestamps)
    valid = stamps.dropna()
    if valid.empty:
        return np.zeros(len(stamps), dtype=bool)
    known = holidays(range(int(valid.dt.year.min()), int(valid.dt.year.max()) + 1))
    return stamps.dt.normalize().isin(pd.to_datetime(sorted(known))).to_numpy()


def working_days(timestamps: pd.Series) -> np.ndarray:
    """True for Monday to Friday when it is not a holiday."""
    stamps = pd.to_datetime(timestamps)
    return (stamps.dt.dayofweek.to_numpy() < 5) & ~holiday_flags(stamps)
