from sqlalchemy import select
import pandas as pd
from datetime import date, timedelta

from database import metadata, connection


def get_performances(
    system_id: int, col: str, start_dt: date = None, end_dt: date = None
):
    prfms = metadata.tables["performances"]
    stmt = select(prfms.c.date, prfms.c[col]).where(prfms.c.system_id == system_id)
    if start_dt is not None and end_dt is not None:
        stmt = stmt.where(start_dt <= prfms.c.date).where(prfms.c.date < end_dt)
    rslt = connection.execute(stmt)
    df = pd.DataFrame(rslt.all(), columns=rslt.keys())
    dct = df.to_dict("list")

    return dct


# def get_performances_period(system_id: int, col: str, start_dt, end_dt):
#     prfms = metadata.tables["performances"]
#     stmt = (
#         select(prfms.c.date, prfms.c[col])
#         .where(prfms.c.system_id == system_id)
#         .where(start_dt <= prfms.c.date)
#         .where(prfms.c.date < end_dt)
#     )
#     rslt = connection.execute(stmt)
#     df = pd.DataFrame(rslt.all(), columns=rslt.keys())
#     dct = df.to_dict("list")

#     return dct


def get_temps(system_id: int, start_dt: date, end_dt: date):
    obs = metadata.tables["observations"]
    tmps = metadata.tables["t_mods"]
    stmt = (
        select(obs.c.datetime, tmps.c.t_mod, tmps.c.t_noct)
        .join(obs)
        .where(tmps.c.system_id == system_id)
        .where(start_dt <= obs.c.datetime)
        .where(obs.c.datetime < end_dt)
    )
    rslt = connection.execute(stmt)
    df = pd.DataFrame(rslt.all(), columns=rslt.keys())
    dct = df.to_dict("list")

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
    dct = df.to_dict("list")

    return dct


def get_inverters(system_id: int, col: str, start_dt: date, end_dt: date):
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
    dct = df.to_dict("list")

    return dct


if __name__ == "__main__":
    rslt = get_temps(date(2021, 5, 31), 1)
    print(rslt)
