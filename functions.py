from datetime import date, timedelta, datetime
from dateutil.relativedelta import relativedelta
from pandas import DataFrame, to_datetime, Grouper

from typing import List


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


def groupby(df: DataFrame, freq: str) -> DataFrame:
    df.dropna(inplace=True)
    df["date"] = to_datetime(df["date"])

    df = df.groupby(Grouper(key="date", freq=freq)).sum().reset_index()
    if freq == "MS":
        df["date"] = df["date"].dt.strftime("%Y-%m")
    if freq == "YS":
        df["date"] = df["date"].dt.strftime("%Y")
    return df


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
