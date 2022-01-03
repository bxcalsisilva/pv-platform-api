from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from datetime import date
import json
from typing import List, Optional
from numpy import e
import pandas
from sqlalchemy.sql.functions import func

import crud
import functions
from enums import (
    Inverters,
    Aggregations,
    Technologies,
    Yields,
    Comparations,
    Efficiencies,
    Energies,
)

config = json.load(open("config.json", "r"))

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def main():
    return "PV Platform API"


@app.get("/locations")
def get_locations() -> dict:
    """Read the available location in the Database.

    Returns:
        list: dictionaries with location_id and label keys.
        e.g.
        [
            {
                "location_id": 1,
                "label": "[City] - [Label]"
            },
            ...
        ]
    """
    return crud.get_locs()


@app.get("/location/{loc_id}/systems")
def get_systems_by_location(loc_id: int) -> list:
    """Read system availabel in the selected location

    Args:
        loc_id (integer): Location id from database, e.g. 1 - 6

    Returns:
        list: dictionaries with location_id, system_id and technology of each system.
        e.g.
        [
            {
                "location_id": 1,
                "system_id": 1,
                "technology": "PERC"
            },
            ...
        ]
    """
    return crud.get_sys(loc_id)


@app.get("/location/{loc_id}/system/{sys_id}")
def get_system_information(loc_id: int, sys_id: int) -> list:
    """Get system information on module and system level

    Args:
        sys_id (integer): System id, i.e. 1 - 18 (3 system per location)

    Returns:
        list: System and module lists of information dictionaries.
        e.g.
        [
            [
                {
                    "x": "Nominal Power (kW)",
                    "y": 1.675
                },
                ...
            ],
            [
                {
                    "x": "Nominal Power (kW)",
                    "y": 0.335
                },
                ...
            ]
        ]
    """
    return crud.get_sys_info(sys_id), crud.get_tech_info(sys_id)


@app.get("/location/{loc_id}/system/{sys_id}/irr/start_dt={start_dt}")
def get_irradiance(loc_id: int, sys_id: int, start_dt: str, end_dt: str = None) -> list:
    dates = functions.format_dates(start_dt, end_dt)
    dates = functions.sort_dates(dates)
    dates = functions.set_dates_range(dates)

    return crud.get_irrs(loc_id, dates)


@app.get("/location/{loc_id}/system/{sys_id}/t_mod/start_dt={start_dt}")
def get_module_temperature(
    loc_id: int, sys_id: int, start_dt: str, end_dt: str = None
) -> list:
    dates = functions.format_dates(start_dt, end_dt)
    dates = functions.sort_dates(dates)
    dates = functions.set_dates_range(dates)

    return crud.get_temps(sys_id, dates)


@app.get("/location/{loc_id}/system/{sys_id}/inverter/{col}/start_dt={start_dt}")
def get_system_output(
    loc_id: int, sys_id: int, col: Inverters, start_dt: str, end_dt: str = None
) -> list:
    dates = functions.format_dates(start_dt, end_dt)
    dates = functions.sort_dates(dates)
    dates = functions.set_dates_range(dates)

    return crud.get_invs(sys_id, col.name, dates)


@app.get("/location/{loc_id}/system/{sys_id}/yield/{col}/start_dt={start_dt}")
def get_yield(
    loc_id: int,
    sys_id: int,
    col: Yields,
    start_dt: str,
    end_dt: Optional[str] = None,
    agg: Optional[Aggregations] = Aggregations.D,
):
    dates = functions.format_dates(start_dt, end_dt)
    dates = functions.sort_dates(dates)
    dates = functions.set_dates_range(dates, mode=agg.value)

    yields = crud.get_perfs(sys_id, col.name, dates)
    try:
        df = functions.groupby(yields, freq=agg.name)
        df.columns = ["date", "value"]
    except ValueError:
        return {}

    return df.to_dict("records")


@app.get("/location/{loc_id}/system/{sys_id}/performance-ratio/start_dt={start_dt}")
def get_performance_ratio(
    loc_id: int,
    sys_id: int,
    start_dt: str,
    end_dt: Optional[str] = None,
    agg: Aggregations = Aggregations.D,
):
    dates = functions.format_dates(start_dt, end_dt)
    dates = functions.sort_dates(dates)
    dates = functions.set_dates_range(dates, mode=agg.value)

    yield_final = crud.get_perfs(sys_id, "yield_final", dates)
    yield_reference = crud.get_perfs(sys_id, "yield_reference", dates)

    yields = pandas.merge(yield_final, yield_reference, on="date")
    yields.columns = ["date", "final", "reference"]
    yields[["final", "reference"]] = yields[["final", "reference"]].astype("float")

    try:
        df = functions.groupby(yields, freq=agg.name)
        df["performance_ratio"] = df["final"] / df["reference"]
        df.rename({"performance_ratio": "value"}, axis=1, inplace=True)
        df.dropna(inplace=True)
    except ValueError:
        return {}

    return df[["date", "value"]].to_dict("records")


@app.get("/location/{loc_id}/system/{sys_id}/efficiency/inverter/start_dt={start_dt}")
def get_inverter_efficiency(
    loc_id: int,
    sys_id: int,
    start_dt: str,
    end_dt: Optional[str] = None,
    agg: Aggregations = Aggregations.D,
):
    dates = functions.format_dates(start_dt, end_dt)
    dates = functions.sort_dates(dates)
    dates = functions.set_dates_range(dates, agg.value)

    energy_dc = crud.get_perfs(sys_id, "energy_dc", dates)
    energy_ac = crud.get_perfs(sys_id, "energy_ac", dates)

    energy = pandas.merge(energy_dc, energy_ac, on="date")
    energy.columns = ["date", "dc", "ac"]
    energy[["dc", "ac"]] = energy[["dc", "ac"]].astype(float)

    try:
        df = functions.groupby(energy, freq=agg.name)

        df["efficiency_inverter"] = df["ac"] / df["dc"] * 100
        df.rename({"efficiency_inverter": "value"}, axis=1, inplace=True)
        df.dropna(inplace=True)
    except ValueError:
        return {}

    return df[["date", "value"]].to_dict("records")


@app.get("/location/{loc_id}/system/{sys_id}/efficiency/{col}/start_dt={start_dt}")
def get_efficiency(
    loc_id: int,
    sys_id: int,
    col: Efficiencies,
    start_dt: str,
    end_dt: Optional[str] = None,
    agg: Aggregations = Aggregations.D,
):
    dates = functions.format_dates(start_dt, end_dt)
    dates = functions.sort_dates(dates)
    dates = functions.set_dates_range(dates, agg.value)

    energy_col = config["efficiencies"][col.name]

    energy = crud.get_perfs(sys_id, energy_col, dates)
    yield_reference = crud.get_perfs(sys_id, "yield_reference", dates)
    system_area = float(crud.system_area(sys_id))

    df = pandas.merge(energy, yield_reference, on="date")
    df.columns = ["date", "energy", "reference"]
    df[["energy", "reference"]] = df[["energy", "reference"]].astype("float")

    try:
        df = functions.groupby(df, freq=agg.name)

        df["efficiency"] = (df["energy"] * 100) / (df["reference"] * system_area)
        df.rename({"efficiency": "value"}, axis=1, inplace=True)
        df.dropna(inplace=True)
    except ValueError:
        return {}

    return df[["date", "value"]].to_dict("records")


@app.get("/location/{loc_id}/system/{sys_id}/energy/{col}/start_dt={start_dt}")
def get_energy(
    loc_id: int,
    sys_id: int,
    col: Energies,
    start_dt: str,
    end_dt: Optional[str] = None,
    agg: Aggregations = Aggregations.D,
):
    dates = functions.format_dates(start_dt, end_dt)
    dates = functions.sort_dates(dates)
    dates = functions.set_dates_range(dates, agg.value)

    energy = crud.get_perfs(sys_id, col.name, dates)

    try:
        df = functions.groupby(energy, freq=agg.name)
        df.columns = ["date", "value"]
    except ValueError:
        return {}

    return df.to_dict("records")


@app.get("/comparison/{col}/{start_dt}/{end_dt}/")
def get_comparation(
    col: Comparations,
    start_dt: str,
    end_dt: str,
    techs: List[str] = Query(None),
):
    if techs is None or not techs:
        return {}

    techs = [tech.upper() for tech in techs]

    dates = functions.format_dates(start_dt, end_dt)
    dates = functions.sort_dates(dates)
    dates = functions.set_dates_range(dates)

    rslt = crud.get_perfs_cmp(col.name, dates)
    rslt = rslt.loc[rslt["technology"].isin(techs)]

    dct = functions.format_comparison(rslt)
    return dct
