from datetime import date, timedelta, datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import numpy as np
import json

from typing import List, Dict, Optional
from decimal import Decimal

from pandas.core.frame import DataFrame

import crud


def format_date(date: str) -> date:
    try:
        return datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        try:
            return datetime.strptime(date, "%Y-%m")
        except ValueError:
            try:
                return datetime.strptime(date, "%Y")
            except:
                print(f"Date format unknown {date}")
                raise ValueError


def format_dates(start_date: str, end_date: str) -> List[date]:
    start_date = format_date(start_date)
    if end_date:
        end_date = format_date(end_date)

    return [start_date, end_date]


def sort_dates(dates: List[date]) -> List[date]:
    dates = sorted(dates, key=lambda x: (x is None, x))
    if dates[1] is None:
        dates[1] = dates[0]

    return dates


def set_dates_range(dates: List[date], mode: str = "date") -> List[date]:
    dates[0], dates[1] = dates[0], dates[1]
    if mode == "date":
        dates[1] += timedelta(days=1)
    elif mode == "week":
        dates[0] = dates[0] - timedelta(days=dates[0].weekday())
        dates[1] = dates[1] - timedelta(days=dates[1].weekday()) + timedelta(days=7)
    elif mode == "month":
        dates[0] = dates[0].replace(day=1)
        dates[1] = dates[1].replace(day=1) + relativedelta(months=1)
    elif mode == "year":
        dates[0] = dates[0].replace(month=1, day=1)
        dates[1] = dates[1].replace(month=1, day=1) + relativedelta(years=1)
    else:
        print("mode not supported:", mode)
        raise ValueError

    return dates


def groupby(df: pd.DataFrame, freq: str) -> pd.DataFrame:
    df.dropna(inplace=True)
    df["date"] = pd.to_datetime(df["date"])

    df = df.groupby(pd.Grouper(key="date", freq=freq)).sum().reset_index()
    return df


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


def format_date(date: str) -> date:
    try:
        return datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        try:
            return datetime.strptime(date, "%Y-%m")
        except ValueError:
            try:
                return datetime.strptime(date, "%Y")
            except:
                print(f"Date format unknown {date}")
                raise ValueError


def format_dates(start_date: str, end_date: str = None) -> List[date]:
    start_date = format_date(start_date)
    if end_date:
        end_date = format_date(end_date)

    return [start_date, end_date]


def aggregate_performance(data: List[Dict[str, Decimal]], mode: str):

    df = pd.DataFrame(data)
    df["date"] = pd.to_datetime(df["date"])

    pass
