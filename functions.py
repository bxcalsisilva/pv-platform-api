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

    counts = df.groupby(Grouper(key="date", freq=freq)).count().iloc[:, 0]
    counts = list(counts)

    df = df.groupby(Grouper(key="date", freq=freq)).sum().reset_index()
    if freq == "MS":
        df["date"] = df["date"].dt.strftime("%Y-%m")
        df["text"] = [f"days: {d}" for d in counts]
    if freq == "YS":
        df["date"] = df["date"].dt.strftime("%Y")
        df["text"] = [f"days: {d}" for d in counts]
    return df


def format_comparison(rslt: DataFrame):

    locations = ["PUCP", "UNI", "UNTRM", "UNSA", "UNAJ", "UNJBG"]
    techs = ["PERC", "HIT", "CIGS"]

    data = {}

    for loc in locations:
        df_location = rslt.loc[rslt["label"] == loc]

        techs = df_location["technology"].to_list()
        avg = df_location["avg"].to_list()
        se = df_location["se"].to_list()
        days = ("days: " + df_location["days"].astype(str)).to_list()
        data[loc] = {"techs": techs, "avg": avg, "se": se, "days": days}

    return data
