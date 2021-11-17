from fastapi import FastAPI
from datetime import date

import crud

app = FastAPI()


@app.get("/")
def main():
    return "Hello World"


@app.get("/irr")
def read_irradiances(date: date, loc_id: int):
    return crud.get_irr(date, loc_id)


@app.get("/temps")
def read_temperatures(date: date, system_id: int):
    return crud.get_temps(date, system_id)


@app.get("/inv")
def read_irradiances(date: date, system_id: int, column: str):
    return crud.get_inverters(date, system_id, column)


@app.get("/perf")
def read_performances(system_id: int, column: str):
    return crud.get_performances(system_id, column)
