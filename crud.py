from sqlalchemy import select
import pandas as pd
from pandas import DataFrame
from datetime import date, timedelta
import json

from database import metadata, connection

config = json.load(open("config.json", "r"))


def get_locs():
    locs = metadata.tables["locations"]
    stmt = select(locs.c.location_id, locs.c.label, locs.c.city)
    rslt = connection.execute(stmt)
    df = pd.DataFrame(rslt.all(), columns=rslt.keys())
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
    stmt = select(
        sys.c.nominal_power,
        sys.c.area,
        sys.c.row,
        sys.c.parallel,
        sys.c.commisioned,
        sys.c.inclination,
        sys.c.orientation,
        sys.c.azimuth,
    )
    stmt = stmt.where(sys_id == sys.c.system_id)
    rslt = connection.execute(stmt)
    df = pd.DataFrame(rslt.all(), columns=config["sys_info_cols"])
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
    dct = dict_format(df)

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
    dct = dict_format(df)

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
    dct = dict_format(df)

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
    dct = dict_format(df)

    return dct


def dict_format(df: DataFrame):
    df.columns = ["x", "y"]
    dct = df.to_dict("records")
    return dct


if __name__ == "__main__":
    # rslt = get_temps(1, date(2021, 5, 31), date(2021, 6, 1))
    rslt = get_sys_info(1)
    print(rslt)
