from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from typing import Dict, List, Optional
import pandas

import crud
import functions
from enums import (
    Inverters,
    Aggregations,
    Yields,
    Comparations,
    Efficiencies,
    Energies,
    PerformanceRatios,
)

description = """
PV-Platform API helps retrieve the monitoring and performance metrics of systems installed in Peru.

## Locations & Systems

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

app = FastAPI(title="PV-Platform API", description=description, version="0.11.2")

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


@app.get("/locations", tags=["Description"])
def get_locations() -> List[Dict[str, str]]:
    """
    Read the available locations in the Database.

    Returns:
    - List[Dict[str, str]]: Dictionaries with location_id and label keys.
    """
    return crud.get_locs()


@app.get("/location/{loc_id}/systems", tags=["Description"])
def get_systems_by_location(loc_id: int) -> List[Dict[str, int]]:
    """Read systems available in the requested location.

    Args:
    - loc_id (integer): Location ID.

    Returns:
    - List[Dict[str, int]]: dictionaries with location_id, system_id and technology of each system.
    """
    return crud.get_sys(loc_id)


@app.get("/location/{loc_id}/system/{sys_id}", tags=["Description"])
def get_system_information(loc_id: int, sys_id: int) -> List[List[Dict[str, float]]]:
    """
    Get system information on module and system level.

    Args:
    - loc_id (int): Location ID
    - sys_id (int): System ID

    Returns:
    - List[List[Dict[str, float]]]: System and module list of information dictionaries.
    """
    return crud.get_sys_info(sys_id), crud.get_tech_info(sys_id)


@app.get("/ambient/irr/{loc_id}/{start_dt}", tags=["Ambient"])
def get_irradiance(
    loc_id: int, start_dt: str, end_dt: str = None
) -> Dict[str, List[float]]:
    """Get Irradiance from database.

    Args:
    - loc_id (int): Location ID.
    - start_dt (str): Start date.
    - end_dt (str, optional): End date. Defaults to None.

    Returns:
    - Dict[str, List[float]]: Measurements on a minute basis.
    """
    dates = functions.format_dates(start_dt, end_dt)
    dates = functions.sort_dates(dates)
    dates = functions.set_dates_range(dates)

    return crud.get_irrs(loc_id, dates)


@app.get("/ambient/t_mod/{sys_id}/{start_dt}", tags=["Ambient"])
def get_module_temperature(
    sys_id: int, start_dt: str, end_dt: str = None
) -> Dict[str, List[float]]:
    """Get module temperature of a system.

    Args:
    - sys_id (int): System ID.
    - start_dt (str): Start date.
    - end_dt (str, optional): End date. Defaults to None.

    Returns:
    - Dict[str, List[float]]: Module temperature on a minute basis.
    """
    dates = functions.format_dates(start_dt, end_dt)
    dates = functions.sort_dates(dates)
    dates = functions.set_dates_range(dates)

    return crud.get_temps(sys_id, dates)


@app.get("/inverter/{col}/{sys_id}/{start_dt}", tags=["Inverter"])
def get_system_output(
    sys_id: int, col: Inverters, start_dt: str, end_dt: str = None
) -> Dict[str, List[float]]:
    """Get array or system electrical output.

    Args:
    - loc_id (int): Location ID.
    - sys_id (int): SystemID.
    - col (Inverters): Electrical output selection.
    - start_dt (str): Start date.
    - end_dt (str, optional): End date. Defaults to None.

    Returns:
    - Dict[str, List[float]]: Array or System electrical output on a minute basis.
    """
    dates = functions.format_dates(start_dt, end_dt)
    dates = functions.sort_dates(dates)
    dates = functions.set_dates_range(dates)

    return crud.get_invs(sys_id, col.name, dates)


@app.get("/yield/{col}/{sys_id}/{start_dt}", tags=["Yield"])
def get_yield(
    sys_id: int,
    col: Yields,
    start_dt: str,
    end_dt: Optional[str] = None,
    agg: Optional[Aggregations] = Aggregations.D,
) -> Dict[str, List[float]]:
    """Get system yield.

    Args:
    - sys_id (int): System ID.
    - col (Yields): Yield selection.
    - start_dt (str): Start date.
    - end_dt (Optional[str], optional): End date. Defaults to None.
    - agg (Optional[Aggregations], optional): Aggregation selection. Defaults to Aggregations.D.

    Returns:
    - Dict[str, List[float]]: Yield on a daily basis.
    """
    dates = functions.format_dates(start_dt, end_dt)
    dates = functions.sort_dates(dates)
    dates = functions.set_dates_range(dates, mode=agg.value)

    yields = crud.get_perfs(sys_id, col.name, dates)
    try:
        df = functions.groupby(yields, freq=agg.name)
        df.rename({"date": "x", col.name: "y"}, axis=1, inplace=True)
    except ValueError:
        return {}

    return df.to_dict("list")


@app.get("/performance-ratio/{col}/{sys_id}/{start_dt}", tags=["Performance ratio"])
def get_performance_ratio(
    col: PerformanceRatios,
    sys_id: int,
    start_dt: str,
    end_dt: Optional[str] = None,
    agg: Aggregations = Aggregations.D,
) -> Dict[str, List[float]]:
    """Get system performance ratio.

    Args:
    - col (PerformanceRatios): AC or DC definition.
    - sys_id (int): System ID.
    - start_dt (str): Start date.
    - end_dt (Optional[str], optional): End date. Defaults to None.
    - agg (Aggregations, optional): Aggregation selection. Defaults to Aggregations.D.

    Returns:
    - Dict[str, List[float]]: Performance ratio on a daily basis.
    """
    dates = functions.format_dates(start_dt, end_dt)
    dates = functions.sort_dates(dates)
    dates = functions.set_dates_range(dates, mode=agg.value)

    yield_name = col.name

    yield_col = crud.get_perfs(sys_id, yield_name, dates)
    yield_reference = crud.get_perfs(sys_id, "yield_reference", dates)

    yields = pandas.merge(yield_col, yield_reference, on="date")
    yields.columns = ["date", yield_name, "reference"]
    yields[[yield_name, "reference"]] = yields[[yield_name, "reference"]].astype(
        "float"
    )

    try:
        df = functions.groupby(yields, freq=agg.name)
        df["performance_ratio"] = df[yield_name] / df["reference"]
        df.rename({"performance_ratio": "y", "date": "x"}, axis=1, inplace=True)
        df.dropna(inplace=True)
    except ValueError:
        return {}

    try:
        return df[["x", "y", "text"]].to_dict("list")
    except:
        return df[["x", "y"]].to_dict("list")


@app.get("/efficiency/inverter/{sys_id}/{start_dt}", tags=["Efficiency"])
def get_inverter_efficiency(
    sys_id: int,
    start_dt: str,
    end_dt: Optional[str] = None,
    agg: Aggregations = Aggregations.D,
) -> Dict[str, List[float]]:
    """Get inverter efficiency.

    Args:
    - sys_id (int): System ID.
    - start_dt (str): Start date.
    - end_dt (Optional[str], optional): End date. Defaults to None.
    - agg (Aggregations, optional): Aggregation selection. Defaults to Aggregations.D.

    Returns:
    - Dict[str, List[float]]: Get inverter efficiency on a dialy basis.
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
        df.rename({"efficiency_inverter": "y", "date": "x"}, axis=1, inplace=True)
        df.dropna(inplace=True)
    except ValueError:
        return {}

    try:
        return df[["x", "y", "text"]].to_dict("list")
    except:
        return df[["x", "y"]].to_dict("list")


@app.get("/efficiency/{col}/{sys_id}/{start_dt}", tags=["Efficiency"])
def get_efficiency(
    col: Efficiencies,
    sys_id: int,
    start_dt: str,
    end_dt: Optional[str] = None,
    agg: Aggregations = Aggregations.D,
) -> Dict[str, List[float]]:
    """Get array of system efficiency.

    Args:
    - col (Efficiencies): Array or System.
    - sys_id (int): System ID.
    - start_dt (str): Start date.
    - end_dt (Optional[str], optional): End date. Defaults to None.
    - agg (Aggregations, optional): Aggregation selection. Defaults to Aggregations.D.

    Returns:
    - Dict[str, List[float]]: Efficiency on a daily basis.
    """
    dates = functions.format_dates(start_dt, end_dt)
    dates = functions.sort_dates(dates)
    dates = functions.set_dates_range(dates, agg.value)

    energy = crud.get_perfs(sys_id, col.name, dates)
    yield_reference = crud.get_perfs(sys_id, "yield_reference", dates)
    system_area = float(crud.system_area(sys_id))

    df = pandas.merge(energy, yield_reference, on="date")
    df.columns = ["date", "energy", "reference"]
    df[["energy", "reference"]] = df[["energy", "reference"]].astype("float")

    try:
        df = functions.groupby(df, freq=agg.name)

        df["efficiency"] = (df["energy"] * 100) / (df["reference"] * system_area)
        df.rename({"efficiency": "y", "date": "x"}, axis=1, inplace=True)
        df.dropna(inplace=True)
    except ValueError:
        return {}

    try:
        return df[["x", "y", "text"]].to_dict("list")
    except:
        return df[["x", "y"]].to_dict("list")


@app.get("/energy/{col}/{sys_id}/{start_dt}", tags=["Energy"])
def get_energy(
    col: Energies,
    sys_id: int,
    start_dt: str,
    end_dt: Optional[str] = None,
    agg: Aggregations = Aggregations.D,
) -> Dict[str, List[float]]:
    """Get DC or AC energy.

    Args:
    - col (Energies): DC or AC.
    - sys_id (int): System ID.
    - start_dt (str): Start date.
    - end_dt (Optional[str], optional): End date. Defaults to None.
    - agg (Aggregations, optional): Aggregation selection. Defaults to Aggregations.D.

    Returns:
    - Dict[str, List[float]]: Energy on a daily basis.
    """
    dates = functions.format_dates(start_dt, end_dt)
    dates = functions.sort_dates(dates)
    dates = functions.set_dates_range(dates, agg.value)

    energy = crud.get_perfs(sys_id, col.name, dates)

    try:
        df = functions.groupby(energy, freq=agg.name)
        df.rename({"date": "x", col.name: "y"}, axis=1, inplace=True)
    except ValueError:
        return {}

    return df.to_dict("list")


@app.get("/comparison/{col}/{start_dt}/{end_dt}/", tags=["Comparison"])
def get_comparation(
    col: Comparations,
    start_dt: str,
    end_dt: str,
) -> Dict[str, List[float]]:
    """Get system comparations.

    Args:
    - col (Comparations): Performance metric selection.
    - start_dt (str): Start date.
    - end_dt (str): End date.

    Returns:
    - Dict[str, List[float]]: System performance metric and confidence level.
    """

    dates = functions.format_dates(start_dt, end_dt)
    dates = functions.sort_dates(dates)
    dates = functions.set_dates_range(dates)

    rslt = crud.get_perfs_cmp(col.name, dates)
    rslt.fillna("null", inplace=True)

    print(rslt)

    dct = functions.format_comparison(rslt)

    return dct
