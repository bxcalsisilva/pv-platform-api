from sqlalchemy import select
import pandas as pd
from pandas import DataFrame
from datetime import date, timedelta

from database import metadata, connection


def get_perfs(system_id: int, col: str, start_dt: date = None, end_dt: date = None):
    prfms = metadata.tables["performances"]
    stmt = select(prfms.c.date, prfms.c[col]).where(prfms.c.system_id == system_id)
    if start_dt is not None and end_dt is not None:
        stmt = stmt.where(start_dt <= prfms.c.date).where(prfms.c.date < end_dt)
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
        .where(obs.c.datetime < end_dt)
    )
    rslt = connection.execute(stmt)
    df = pd.DataFrame(rslt.all(), columns=rslt.keys())
    dct = dict_format(df)

    return dct


def get_irrs(loc_id: int, start_dt: date, end_dt: date):
    obs = metadata.tables["observations"]
    irr = metadata.tables["irradiances"]
    stmt = (
        select(obs.c.datetime, irr.c.irradiance)
        .join(obs)
        .where(irr.c.location_id == loc_id)
        .where(start_dt <= obs.c.datetime)
        .where(obs.c.datetime < end_dt)
    )
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
        .where(obs.c.datetime < end_dt)
    )
    rslt = connection.execute(stmt)
    df = pd.DataFrame(rslt.all(), columns=rslt.keys())
    dct = dict_format(df)

    return dct


def dict_format(df: DataFrame):
    df.columns = ["x", "y"]
    dct = df.to_dict("records")
    return dct


if __name__ == "__main__":
    rslt = get_temps(1, date(2021, 5, 31), date(2021, 6, 1))
    print(rslt)
