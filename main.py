from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from datetime import date, timedelta

import crud

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


@app.get("/locs")
def read_locs():
    return crud.get_locs()


@app.get("/sys")
def read_sys(loc_id: int):
    return crud.get_sys(loc_id)


@app.get("/sys/")
def read_sys_info(sys_id: int):
    return crud.get_sys_info(sys_id)


@app.get("/irrs/{loc_id}")
def read_irrs(loc_id: int, start_dt: date, end_dt: date = None):
    if end_dt:
        end_dt = end_dt + timedelta(days=1)
        return crud.get_irrs(loc_id, start_dt, end_dt)
    else:
        end_dt = start_dt + timedelta(days=1)
        return crud.get_irrs(loc_id, start_dt, end_dt)


@app.get("/temps/{sys_id}")
def read_temps(sys_id: int, start_dt: date, end_dt: date = None):
    if end_dt:
        end_dt = end_dt + timedelta(days=1)
        return crud.get_temps(sys_id, start_dt, end_dt)
    else:
        end_dt = start_dt + timedelta(days=1)
        return crud.get_temps(sys_id, start_dt, end_dt)


@app.get("/invs/{sys_id}/{col}")
def read_invs(sys_id: int, col: str, start_dt: date, end_dt: date = None):
    if end_dt:
        end_dt = end_dt + timedelta(days=1)
        return crud.get_invs(sys_id, col, start_dt, end_dt)
    else:
        end_dt = start_dt + timedelta(days=1)
        return crud.get_invs(sys_id, start_dt, end_dt)


@app.get("/perfs/{sys_id}/{col}")
def read_perfs(sys_id: int, col: str, start_dt: date = None, end_dt: date = None):
    if start_dt and not end_dt:
        end_dt = start_dt + timedelta(days=1)
        return crud.get_perfs(sys_id, col, start_dt, end_dt)
    elif start_dt and end_dt:
        end_dt = end_dt + timedelta(days=1)
        return crud.get_perfs(sys_id, col, start_dt, end_dt)
    else:
        return crud.get_perfs(sys_id, col)
