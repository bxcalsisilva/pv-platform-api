from sqlalchemy import select
from sqlalchemy import func
import pandas as pd
from pandas import DataFrame
from datetime import date
import json
from typing import Dict, List

# from functions import sort_dates, agg_rslt
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
    ).where(loc_id == sys.c.location_id)
    rslt = connection.execute(stmt)
    df = pd.DataFrame(rslt.all(), columns=rslt.keys())
    dct = df.to_dict("records")

    return dct


def get_sys_info(sys_id: int):
    sys = metadata.tables["systems"]
    locs = metadata.tables["locations"]
    stmt = (
        select(
            sys.c.nominal_power,
            sys.c.area * sys.c.row * sys.c.parallel,
            sys.c.row,
            sys.c.parallel,
            sys.c.row * sys.c.parallel,
            sys.c.commisioned,
            sys.c.inclination,
            sys.c.orientation,
            sys.c.azimuth,
            locs.c.latitude,
            locs.c.longitude,
            locs.c.altitude,
        )
        .join(locs)
        .where(sys_id == sys.c.system_id)
    )
    rslt = connection.execute(stmt)
    df = pd.DataFrame(rslt.all(), columns=config["sys_info_cols"])
    df = df.transpose().reset_index()

    dct = dict_format(df)

    return dct


def get_tech_info(sys_id: int):
    sys = metadata.tables["systems"]
    stmt = (
        select(
            sys.c.nominal_power / (sys.c.row * sys.c.parallel),
            sys.c.area,
            sys.c.alpha,
            sys.c.beta,
            sys.c.gamma,
            sys.c.noct,
            sys.c.efficiency,
        )
        .where(sys_id == sys.c.system_id)
        .distinct()
    )
    rslt = connection.execute(stmt)
    df = pd.DataFrame(rslt.all(), columns=config["tech_info_cols"])
    df = df.transpose().reset_index()

    dct = dict_format(df)

    return dct


def get_perfs(system_id: int, col: str, dates: List[date]) -> pd.DataFrame:
    performances_table = metadata.tables["performances"]
    stmt = (
        select(performances_table.c.date, performances_table.c[col])
        .where(performances_table.c.system_id == system_id)
        .where(dates[0] <= performances_table.c.date)
        .where(performances_table.c.date < dates[1])
        .distinct()
    )
    rslt = connection.execute(stmt)
    df = pd.DataFrame(rslt.all(), columns=rslt.keys())

    return df


def get_perfs_cmp(col: str, dates: List[date]):
    prfms = metadata.tables["performances"]
    locs = metadata.tables["locations"]
    sys = metadata.tables["systems"]
    stmt = (
        select(
            locs.c.label,
            sys.c.technology,
            func.avg(prfms.c[col]).label("avg"),
            (func.stddev(prfms.c[col]) / func.sqrt(func.count(prfms.c[col]))).label(
                "se"
            ),
        )
        .join(sys, sys.c.system_id == prfms.c.system_id)
        .join(locs, locs.c.location_id == sys.c.location_id)
        .where(dates[0] <= prfms.c.date)
        .where(prfms.c.date < dates[1])
        .group_by(sys.c.system_id)
    )
    rslt = connection.execute(stmt)
    df = pd.DataFrame(rslt.all(), columns=rslt.keys())

    return df


def get_temps(system_id: int, dates: List[date]):
    obs = metadata.tables["observations"]
    tmps = metadata.tables["t_mods"]
    stmt = (
        select(obs.c.datetime, tmps.c.t_mod)
        .join(obs)
        .where(tmps.c.system_id == system_id)
        .where(dates[0] <= obs.c.datetime)
        .where(obs.c.datetime < dates[1])
        .distinct()
    )
    rslt = connection.execute(stmt)
    df = pd.DataFrame(rslt.all(), columns=["x", "y"])
    dct = dict_format(df)
    return dct


def get_irrs(loc_id: int, dates: List[date]):
    obs = metadata.tables["observations"]
    irr = metadata.tables["irradiances"]
    stmt = (
        select(obs.c.datetime, irr.c.irradiance)
        .join(obs)
        .where(irr.c.location_id == loc_id)
        .where(dates[0] <= obs.c.datetime)
        .where(obs.c.datetime < dates[1])
        .distinct()
    )
    rslt = connection.execute(stmt)
    df = pd.DataFrame(rslt.all(), columns=["x", "y"])
    dct = dict_format(df)

    return dct


def get_invs(system_id: int, col: str, dates: List[date]):
    obs = metadata.tables["observations"]
    inv = metadata.tables["inverters"]
    stmt = (
        select(obs.c.datetime, inv.c[col])
        .join(obs)
        .where(inv.c.system_id == system_id)
        .where(dates[0] <= obs.c.datetime)
        .where(obs.c.datetime < dates[1])
        .distinct()
    )
    rslt = connection.execute(stmt)
    df = pd.DataFrame(rslt.all(), columns=["x", "y"])
    dct = dict_format(df)

    return dct


def dict_format(df: DataFrame) -> Dict[str, List]:
    dct = df.to_dict("list")
    return dct


def system_area(system_id: int) -> float:
    systems_table = metadata.tables["systems"]
    stmt = select(
        systems_table.c.area * systems_table.c.row * systems_table.c.parallel
    ).where(systems_table.c.system_id == system_id)

    rslt = connection.execute(stmt)

    [sys_area] = rslt.one()
    return sys_area


if __name__ == "__main__":
    loc_id = 1
    sys_id = 1

    start_dt = date(2021, 5, 1)
    end_dt = date(2021, 5, 2)
    agg = "day"

    df = get_irrs(loc_id, [start_dt, end_dt])

    print(df)
