from sqlalchemy import select
from sqlalchemy import func
import pandas as pd
from pandas import DataFrame
from datetime import date, timedelta
from dateutil import relativedelta
import json

from database import metadata, connection

config = json.load(open("config.json", "r"))


def get_locs():
    locs = metadata.tables["locations"]
    stmt = select(locs.c.location_id, func.concat(locs.c.city, " - ", locs.c.label))
    rslt = connection.execute(stmt)
    df = pd.DataFrame(rslt.all(), columns=["location_id", "label"])
    dct = df.to_dict("records")
    return dct


def get_sys(loc_id: int):
    sys = metadata.tables["systems"]
    stmt = select(
        sys.c.location_id,
        sys.c.system_id,
        sys.c.technology,
    )
    stmt = stmt.where(loc_id == sys.c.location_id)
    rslt = connection.execute(stmt)
    df = pd.DataFrame(rslt.all(), columns=rslt.keys())
    dct = df.to_dict("records")

    return dct


def get_sys_info(sys_id: int):
    sys = metadata.tables["systems"]
    locs = metadata.tables["locations"]
    stmt = select(
        sys.c.nominal_power,
        sys.c.row,
        sys.c.parallel,
        sys.c.area * sys.c.row * sys.c.parallel,
        sys.c.row * sys.c.parallel,
        sys.c.commisioned,
        sys.c.inclination,
        sys.c.orientation,
        sys.c.azimuth,
        locs.c.latitude,
        locs.c.longitude,
        locs.c.altitude,
    ).join(locs)
    stmt = stmt.where(sys_id == sys.c.system_id)
    rslt = connection.execute(stmt)
    df = pd.DataFrame(rslt.all(), columns=config["sys_info_cols"])
    df = df.transpose().reset_index()

    dct = dict_format(df)

    return dct


def get_tech_info(sys_id: int):
    sys = metadata.tables["systems"]
    stmt = select(
        sys.c.nominal_power,
        sys.c.area,
        sys.c.alpha,
        sys.c.beta,
        sys.c.gamma,
        sys.c.noct,
        sys.c.efficiency,
    )
    stmt = stmt.where(sys_id == sys.c.system_id)
    rslt = connection.execute(stmt)
    df = pd.DataFrame(rslt.all(), columns=config["tech_info_cols"])
    df = df.transpose().reset_index()

    dct = dict_format(df)

    return dct


def get_perfs(system_id: int, col: str, start_dt: date = None, end_dt: date = None):
    prfms = metadata.tables["performances"]
    stmt = (
        select(prfms.c.date, prfms.c[col])
        .where(prfms.c.system_id == system_id)
        .where(start_dt <= prfms.c.date)
    )
    if end_dt is not None:
        stmt = stmt.where(prfms.c.date < end_dt)
    rslt = connection.execute(stmt)
    df = pd.DataFrame(rslt.all(), columns=rslt.keys())
    dct = dict_format(df, columns=["date", "value"])

    return dct


def get_temps(system_id: int, start_dt: date, end_dt: date):
    obs = metadata.tables["observations"]
    tmps = metadata.tables["t_mods"]
    stmt = (
        select(obs.c.datetime, tmps.c.t_mod)
        .join(obs)
        .where(tmps.c.system_id == system_id)
        .where(start_dt <= obs.c.datetime)
    )
    if end_dt is not None:
        stmt = stmt.where(obs.c.datetime < end_dt)
    rslt = connection.execute(stmt)
    df = pd.DataFrame(rslt.all(), columns=rslt.keys())
    dct = dict_format(df, columns=["date", "value"])

    return dct


def get_irrs(loc_id: int, start_dt: date = None, end_dt: date = None):
    obs = metadata.tables["observations"]
    irr = metadata.tables["irradiances"]
    stmt = (
        select(obs.c.datetime, irr.c.irradiance)
        .join(obs)
        .where(irr.c.location_id == loc_id)
        .where(start_dt <= obs.c.datetime)
    )
    if end_dt is not None:
        stmt = stmt.where(obs.c.datetime < end_dt)
    rslt = connection.execute(stmt)
    df = pd.DataFrame(rslt.all(), columns=rslt.keys())
    dct = dict_format(df, columns=["date", "value"])

    return dct


def get_invs(system_id: int, col: str, start_dt: date, end_dt: date):
    obs = metadata.tables["observations"]
    inv = metadata.tables["inverters"]
    stmt = (
        select(obs.c.datetime, inv.c[col])
        .join(obs)
        .where(inv.c.system_id == system_id)
        .where(start_dt <= obs.c.datetime)
    )
    if end_dt is not None:
        stmt = stmt.where(obs.c.datetime < end_dt)
    rslt = connection.execute(stmt)
    df = pd.DataFrame(rslt.all(), columns=rslt.keys())
    dct = dict_format(df, columns=["date", "value"])

    return dct


def dict_format(df: DataFrame, columns=["x", "y"]):
    df.columns = columns
    dct = df.to_dict("records")
    return dct


def sort_dates(start_dt: date, end_dt: date = None, agg: str = "daily"):
    # create a end_dt in case is none
    if end_dt is None:
        end_dt = start_dt
    # rearrange dates
    if end_dt < start_dt:
        dt_1, dt_2 = end_dt, start_dt
        start_dt, end_dt = dt_1, dt_2

    if agg == "daily":
        end_dt += timedelta(days=1)
    elif agg == "weekly":
        start_dt = start_dt - timedelta(days=start_dt.weekday())
        end_dt = end_dt - timedelta(days=end_dt.weekday()) + timedelta(days=7)
    elif agg == "monthly":
        start_dt = start_dt.replace(day=1)
        end_dt = end_dt.replace(day=1) + relativedelta.relativedelta(months=1)
    elif agg == "yearly":
        start_dt = start_dt.replace(month=1, day=1)
        end_dt = end_dt.replace(year=end_dt.year + 1, month=1, day=1)

    return start_dt, end_dt


if __name__ == "__main__":
    # rslt = get_temps(1, date(2021, 5, 31), date(2021, 6, 1))
    sys_id = 5
    # rslt = get_sys_info(sys_id)
    # print(rslt)

    # rslt = get_tech_info(sys_id)
    # print(rslt)

    start_dt = date(2021, 11, 7)
    end_dt = date(2021, 12, 11)
    rslt = sort_dates(start_dt, end_dt, "weekly")
    print(rslt)
