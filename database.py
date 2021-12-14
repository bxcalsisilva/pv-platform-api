from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy.orm import sessionmaker
import json

config = json.load(open("config.json", "r"))

SQLALCHEMY_DATABASE_URL = f"mariadb+mariadbconnector://{config['usr']}:{config['pwd']}@{config['host']}:{config['port']}/{config['db']}"

engine = create_engine(SQLALCHEMY_DATABASE_URL, future=True, pool_pre_ping=True)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

session = SessionLocal()

connection = engine.connect()

metadata = MetaData()
with engine.connect() as conn:
    metadata.reflect(conn)
