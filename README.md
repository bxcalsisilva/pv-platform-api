# PV Platform API

API for PV website, build with fastAPI.

## Structure

main.py
- API webpage configuration and management

enum.py
- Predefined list of values for API calls

crud.py
- Create, Read, Update, Delete SQL operations

functions.py
- Miscelanion and helpful functions
    - Date formatting, date range format and sorting of dates

database.py
- Connection to database, metadata extraction and opens a session

## Configuration

Edit `config_sample.json` to stablish connection to database.