from datetime import date, timedelta
from dateutil import relativedelta
import pandas as pd
import numpy as np
import json

from pandas.core.frame import DataFrame

import crud


def sort_dates(start_dt: date, end_dt: date = None, agg: str = "day"):
    # create a end_dt in case is none
    if end_dt is None:
        end_dt = start_dt
    # rearrange dates
    if end_dt < start_dt:
        dt_1, dt_2 = end_dt, start_dt
        start_dt, end_dt = dt_1, dt_2

    if agg == "day" or end_dt == "min":
        end_dt += timedelta(days=1)
    elif agg == "week":
        start_dt = start_dt - timedelta(days=start_dt.weekday())
        end_dt = end_dt - timedelta(days=end_dt.weekday()) + timedelta(days=7)
    elif agg == "month":
        start_dt = start_dt.replace(day=1)
        end_dt = end_dt.replace(day=1) + relativedelta.relativedelta(months=1)
    elif agg == "year":
        start_dt = start_dt.replace(month=1, day=1)
        end_dt = end_dt.replace(year=end_dt.year + 1, month=1, day=1)

    return start_dt, end_dt


def agg_rslt(rslt: list, key: str = "date", col: str = "value", freq: str = "T"):
    df = pd.DataFrame(rslt)
    df[key] = pd.to_datetime(df[key])
    groupby = df.groupby([pd.Grouper(key=key, freq=freq)])

    mean = groupby[col].mean()
    std_dev = groupby[col].std()
    count = groupby[col].count()
    std_err = std_dev / np.sqrt(count)

    df = pd.DataFrame()
    df[col] = mean
    df["std_err"] = std_err

    df.dropna(subset=[col], inplace=True)
    df.reset_index(inplace=True)

    return df


def slct_get(loc_id: int, sys_id: int, start_dt: date, end_dt: date, col: str):
    config = json.load(open("config.json", "r"))

    if col == "irr":
        rslt = crud.get_irrs(loc_id, start_dt, end_dt)
    elif col == "t_mod":
        rslt = crud.get_temps(sys_id, start_dt, end_dt)
    elif col in config["inv_cols"]:
        rslt = crud.get_invs(sys_id, col, start_dt, end_dt)
    elif col in config["perf_cols"]:
        rslt = crud.get_perfs(sys_id, col, start_dt, end_dt)

    return rslt


def format_comparison(rslt: DataFrame):
    df = rslt.pivot(index="label", columns="technology", values=["avg", "se"])
    df.columns = df.columns.map("_".join)

    names = df.columns.copy()
    new_names = {"label": "category"}
    for col in names:
        split = col.split("_")
        method, technology = split[0], split[1].lower()
        if method == "avg":
            new_names[col] = technology
        elif method == "se":
            new_names[col] = technology + "_variation"

    df.reset_index(inplace=True)
    df.rename(new_names, axis=1, inplace=True)

    df.fillna("null", inplace=True)

    dct = df.to_dict("records")
    dct = {"data": dct}

    return dct
