from fastapi import FastAPI
from datetime import date, timedelta

import crud

app = FastAPI()


@app.get("/")
def main():
    return "Hello World"


@app.get("/irrs/{loc_id}")
def read_irradiances(loc_id: int, date: date):
    start_dt, end_dt = date, date + timedelta(days=1)
    return crud.get_irrs(loc_id, start_dt, end_dt)


@app.get("/irrs/{loc_id}/{start_dt}_{end_dt}")
def read_irradiances_period(loc_id: int, start_dt: date, end_dt: date):
    return crud.get_irrs(loc_id, start_dt, end_dt + timedelta(days=1))


@app.get("/temps/{system_id}")
def read_temperatures(system_id: int, date: date):
    return crud.get_temps(system_id, date)


@app.get("/temps/{system_id}/{start_dt}_{end_dt}")
def read_temperatures_period(system_id: int, start_dt: date, end_dt: date):
    return crud.get_temps(system_id, start_dt, end_dt + timedelta(days=1))


@app.get("/invs/{system_id}/{column}")
def read_inverters(system_id: int, column: str, date: date):
    start_dt, end_dt = date, date + timedelta(days=1)
    return crud.get_inverters(system_id, column, start_dt, end_dt)


@app.get("/invs/{system_id}/{column}/{start_dt}_{end_dt}")
def read_inverters_period(system_id: int, column: str, start_dt: date, end_dt: date):
    return crud.get_inverters(system_id, column, start_dt, end_dt + timedelta(days=1))


@app.get("/perfs/{system_id}/{column}")
def read_performances(system_id: int, column: str):
    return crud.get_performances(system_id, column)


@app.get("/perfs/{system_id}/{column}/{start_dt}_{end_dt}")
def read_performances_period(system_id: int, column: str, start_dt: date, end_dt: date):
    return crud.get_performances(
        system_id, column, start_dt, end_dt + timedelta(days=1)
    )
