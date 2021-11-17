from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy.orm import sessionmaker

SQLALCHEMY_DATABASE_URL = "mariadb+mariadbconnector://root:password@localhost:3306/test"

engine = create_engine(SQLALCHEMY_DATABASE_URL, future=True)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

session = SessionLocal()

connection = engine.connect()

metadata = MetaData()
with engine.connect() as conn:
    metadata.reflect(conn)
