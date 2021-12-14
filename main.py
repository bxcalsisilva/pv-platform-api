from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from datetime import date, timedelta
import json

import crud
import functions

description = """
## Input values description

Dates must be in the format YYYY-MM-DD

### locs - Locations
There is 6 available locations

### sys - Systems
There are 3 systems per location
sys_id between 1 - 18

e.g. loc_id = 2 -> sys_id = [4, 5, 6]

### irrs - Irradiances

input: loc_id between 1 - 6

### temps - Module temperatures

### invs - Inverter measurements

col: Measured values:
- voltage_dc
- current_dc
- power_apparent
- power_dc
- power_dc_t25
- power_ac
- power_ac_t25

### perfs - Performances

col: Performance metrics:
- radiation
- yield_reference
- yield_absolute
- yield_final
- yield_absolute_t25
- yield_final_t25
- performance_ratio
- performance_ratio_t25
- efficiency_array
- efficiency_system
- efficiency_inverter
- energy_dc
- energy_ac
- energy_dc_t25
- energy_ac_t25

"""

app = FastAPI(description=description)

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


@app.get("/locs")
def read_locs():
    return crud.get_locs()


@app.get("/sys")
def read_sys(loc_id: int):
    return crud.get_sys(loc_id)


@app.get("/sys/")
def read_sys_info(sys_id: int):
    return crud.get_sys_info(sys_id), crud.get_tech_info(sys_id)


# @app.get("/tech/")
# def read_tech_info(sys_id: int):
#     print("read_tech", sys_id)
#     return crud.get_tech_info(sys_id)


@app.get("/irrs/{loc_id}")
def read_irrs(loc_id: int, start_dt: date, end_dt: date = None):
    start_dt, end_dt = crud.sort_dates(start_dt, end_dt)
    return crud.get_irrs(loc_id, start_dt, end_dt)


@app.get("/temps/{sys_id}")
def read_temps(sys_id: int, start_dt: date, end_dt: date = None):
    start_dt, end_dt = crud.sort_dates(start_dt, end_dt)
    return crud.get_temps(sys_id, start_dt, end_dt)


@app.get("/invs/{sys_id}/{col}")
def read_invs(sys_id: int, col: str, start_dt: date, end_dt: date = None):
    start_dt, end_dt = crud.sort_dates(start_dt, end_dt)
    return crud.get_invs(sys_id, col, start_dt, end_dt)


@app.get("/perfs/{sys_id}/{col}")
def read_perfs(sys_id: int, col: str, start_dt: date = None, end_dt: date = None):
    start_dt, end_dt = crud.sort_dates(start_dt, end_dt)
    return crud.get_perfs(sys_id, col, start_dt, end_dt)


@app.get("/info/{loc_id}/{sys_id}/{col}/{agg}/")
def read_info(
    loc_id: int, sys_id: int, col: str, agg: str, start_dt: date, end_dt: date = None
):
    config = json.load(open("config.json", "r"))
    start_dt, end_dt = functions.sort_dates(start_dt, end_dt, agg)
    if col == "irr":
        rslt = crud.get_irrs(loc_id, start_dt, end_dt)
    if col == "t_mod":
        rslt = crud.get_temps(sys_id, start_dt, end_dt)
    if col in config["inv_cols"]:
        rslt = crud.get_invs(sys_id, col, start_dt, end_dt)
    if col in config["perf_cols"]:
        rslt = crud.get_perfs(sys_id, col, start_dt, end_dt)

    freq = config["agg"][agg]

    df = functions.agg_rslt(rslt, freq=freq).fillna("null")

    return df.to_dict("records")
