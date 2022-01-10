from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from datetime import date
import json
from typing import Dict, List, Optional
from numpy import e
import pandas
from pydantic import BaseModel

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

description = """
PV-Platform API helps retrieve the processing of systems installed in Peru.

## Location & Systems

|Location_id|Location|System_id|System|
|---|---|---|---|
|1|PUCP|1|PERC|
|1|PUCP|2|HIT|
|1|PUCP|3|CIGS|
|2|UNI|4|PERC|
|2|UNI|5|HIT|
|2|UNI|6|CIGS|
|3|UNTRM|7|PERC|
|3|UNTRM|8|HIT|
|3|UNTRM|9|CIGS|
|4|UNAJ|10|PERC|
|4|UNAJ|11|HIT|
|4|UNAJ|12|CIGS|
|5|UNJBG|13|PERC|
|5|UNJBG|14|HIT|
|5|UNJBG|15|CIGS|
|6|UNSA|16|PERC|
|6|UNSA|17|HIT|
|6|UNSA|18|CIGS|

## Abbreviations:

- **loc_id**:      Location ID
- **sys_id**:      System ID
- **start_dt**:    Start date (format: YYYY-mm-dd)
- **end_dt**:      End date (format: YYYY-mm-dd)
- **agg**:         Data Aggregation type, e.g. date, month or year

"""

app = FastAPI(title="PV-Platform API", description=description, version="0.10.0")

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
def get_locations() -> List[Dict[str, str]]:
    """
    Read the available location in the Database.

    Returns:
    - List[Dict[str, str]]: Dictionaries with location_id and label keys.
    """
    return crud.get_locs()


@app.get("/location/{loc_id}/systems")
def get_systems_by_location(loc_id: int) -> List[Dict[str, int]]:
    """Read system availabel in the selected location.

    Args:
    - loc_id (integer): Location ID.

    Returns:
    - List[Dict[str, int]]: dictionaries with location_id, system_id and technology of each system.
    """
    return crud.get_sys(loc_id)


@app.get("/location/{loc_id}/system/{sys_id}")
def get_system_information(loc_id: int, sys_id: int) -> List[List[Dict[str, float]]]:
    """
    Get system information on module and system level.

    Args:
    - loc_id (int): Location ID
    - sys_id (int): System ID

    Returns:
    - List[List[Dict[str, float]]]: System and module lists of information dictionaries.
    """
    return crud.get_sys_info(sys_id), crud.get_tech_info(sys_id)


@app.get("/location/{loc_id}/system/{sys_id}/irr/start_dt={start_dt}")
def get_irradiance(
    loc_id: int, sys_id: int, start_dt: str, end_dt: str = None
) -> List[Dict[str, float]]:
    """Get Irradiance from database.

    Args:
    - loc_id (int): Location ID.
    - sys_id (int): System ID.
    - start_dt (str): Start date.
    - end_dt (str, optional): End date. Defaults to None.

    Returns:
    - List[Dict[str, float]]: Measurements on a minute basis.
    """
    dates = functions.format_dates(start_dt, end_dt)
    dates = functions.sort_dates(dates)
    dates = functions.set_dates_range(dates)

    return crud.get_irrs(loc_id, dates)


@app.get("/location/{loc_id}/system/{sys_id}/t_mod/start_dt={start_dt}")
def get_module_temperature(
    loc_id: int, sys_id: int, start_dt: str, end_dt: str = None
) -> List[Dict[str, float]]:
    """Get module temperature of a system.

    Args:
    - loc_id (int): Location ID.
    - sys_id (int): System ID.
    - start_dt (str): Start date.
    - end_dt (str, optional): End date. Defaults to None.

    Returns:
    - List[Dict[str, float]]: Module temperature on a minute basis.
    """
    dates = functions.format_dates(start_dt, end_dt)
    dates = functions.sort_dates(dates)
    dates = functions.set_dates_range(dates)

    return crud.get_temps(sys_id, dates)


@app.get("/location/{loc_id}/system/{sys_id}/inverter/{col}/start_dt={start_dt}")
def get_system_output(
    loc_id: int, sys_id: int, col: Inverters, start_dt: str, end_dt: str = None
) -> List[Dict[str, float]]:
    """Get array or system electrical output.

    Args:
    - loc_id (int): Location ID.
    - sys_id (int): SystemID.
    - col (Inverters): Electrical output selection.
    - start_dt (str): Start date.
    - end_dt (str, optional): End date. Defaults to None.

    Returns:
    - List[Dict[str, float]]: Array or System electrical output on a minute basis.
    """
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
) -> List[Dict[str, float]]:
    """Get system yield.

    Args:
    - loc_id (int): Location ID.
    - sys_id (int): System ID.
    - col (Yields): Yield selection.
    - start_dt (str): Start date.
    - end_dt (Optional[str], optional): End date. Defaults to None.
    - agg (Optional[Aggregations], optional): Aggregation selection. Defaults to Aggregations.D.

    Returns:
    - List[Dict[str, float]]: Yield on a daily basis.
    """
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
) -> List[Dict[str, float]]:
    """Get system performance ratio.

    Args:
    - loc_id (int): Location ID.
    - sys_id (int): System ID.
    - start_dt (str): Start date.
    - end_dt (Optional[str], optional): End date. Defaults to None.
    - agg (Aggregations, optional): Aggregation selection. Defaults to Aggregations.D.

    Returns:
    - List[Dict[str, float]]: Performance ratio on a daily basis.
    """
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
) -> List[Dict[str, float]]:
    """Get inverter efficiency.

    Args:
    - loc_id (int): Location ID.
    - sys_id (int): System ID.
    - start_dt (str): Start date.
    - end_dt (Optional[str], optional): End date. Defaults to None.
    - agg (Aggregations, optional): Aggregation selection. Defaults to Aggregations.D.

    Returns:
    - List[Dict[str, float]]: Get inverter efficiency on a dialy basis.
    """
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
) -> List[Dict[str, float]]:
    """Get array of system efficiency.

    Args:
    - loc_id (int): Location ID.
    - sys_id (int): System ID.
    - col (Efficiencies): Array or System.
    - start_dt (str): Start date.
    - end_dt (Optional[str], optional): End date. Defaults to None.
    - agg (Aggregations, optional): Aggregation selection. Defaults to Aggregations.D.

    Returns:
    - List[Dict[str, float]]: Efficiency on a daily basis.
    """
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
) -> List[Dict[str, float]]:
    """Get DC or AC energy.

    Args:
    - loc_id (int): Location ID.
    - sys_id (int): System ID.
    - col (Energies): DC or AC.
    - start_dt (str): Start date.
    - end_dt (Optional[str], optional): End date. Defaults to None.
    - agg (Aggregations, optional): Aggregation selection. Defaults to Aggregations.D.

    Returns:
    - List[Dict[str, float]]: Energy on a daily basis.
    """
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
) -> List[Dict[str, float]]:
    """Get system comparations.

    Args:
    - col (Comparations): Performance metric selection.
    - start_dt (str): Start date.
    - end_dt (str): End date.
    - techs (List[str], optional): Technology selection. Defaults to Query(None).

    Returns:
    - List[Dict[str, float]]: System performance metric and confidence level.
    """
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


class test_class(BaseModel):
    id: int
    name: str
    number: float


@app.post("/test/")
def test_func(model_class: test_class):
    return {
        "id": model_class.id,
        "name": model_class.name,
        "number": model_class.number,
    }
