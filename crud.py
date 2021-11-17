from sqlalchemy import select
import pandas as pd
from datetime import date, timedelta

from database import metadata, connection


def get_performances(system_id: int, col: str):
    prfms = metadata.tables["performances"]
    stmt = select(prfms.c.date, prfms.c[col]).where(prfms.c.system_id == system_id)
    rslt = connection.execute(stmt)
    df = pd.DataFrame(rslt.all(), columns=rslt.keys())
    dct = df.to_dict("list")

    return dct


def get_temps(date: date, system_id: int):
    obs = metadata.tables["observations"]
    tmps = metadata.tables["t_mods"]
    stmt = (
        select(obs.c.datetime, tmps.c.t_mod, tmps.c.t_noct)
        .join(obs)
        .where(tmps.c.system_id == system_id)
        .where(date <= obs.c.datetime)
        .where(obs.c.datetime < date + timedelta(days=1))
    )
    rslt = connection.execute(stmt)
    df = pd.DataFrame(rslt.all(), columns=rslt.keys())
    dct = df.to_dict("list")

    return dct


def get_irr(date: date, loc_id: int):
    obs = metadata.tables["observations"]
    irr = metadata.tables["irradiances"]
    stmt = (
        select(obs.c.datetime, irr.c.irradiance)
        .join(obs)
        .where(irr.c.loc_id == loc_id)
        .where(date <= obs.c.datetime)
        .where(obs.c.datetime < date + timedelta(days=1))
    )
    rslt = connection.execute(stmt)
    df = pd.DataFrame(rslt.all(), columns=rslt.keys())
    dct = df.to_dict("list")

    return dct


def get_inverters(date: date, system_id: int, col: str):
    obs = metadata.tables["observations"]
    inv = metadata.tables["inverters"]
    stmt = (
        select(obs.c.datetime, inv.c[col])
        .join(obs)
        .where(inv.c.system_id == system_id)
        .where(date <= obs.c.datetime)
        .where(obs.c.datetime < date + timedelta(days=1))
    )
    rslt = connection.execute(stmt)
    df = pd.DataFrame(rslt.all(), columns=rslt.keys())
    dct = df.to_dict("list")

    return dct


if __name__ == "__main__":
    rslt = get_temps(date(2021, 5, 31), 1)
    print(rslt)
