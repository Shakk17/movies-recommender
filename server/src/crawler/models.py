from sqlalchemy import Integer, String, Float
from sqlalchemy import create_engine, Column
from sqlalchemy.ext.declarative import declarative_base

import os

Base = declarative_base()


def db_connect():
    """
    Performs database connection using database settings from settings.py.
    Returns sqlalchemy engine instance
    """
    # Connect to online postgresql database.
    user = os.environ['POSTGRESQL_USER']
    pwd = os.environ['POSTGRESQL_PASSWORD']
    host = os.environ['POSTGRESQL_HOST']
    port = os.environ['POSTGRESQL_PORT']
    db = os.environ['POSTGRESQL_DB']
    database_url = f'postgres://{user}:{pwd}@{host}:{port}/{db}'
    engine = create_engine(database_url)
    return engine


def create_table(engine):
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)


class Movie(Base):
    __tablename__ = "imdb"

    id = Column(Integer, primary_key=True)
    url = Column(String)
    name = Column('name', String)
    genres = Column('genres', String)
    country = Column(String)
    overview = Column(String)
    year = Column(Integer)
    length = Column(Integer)
    popularity_rank = Column('popularity_rank', Integer)
    n_ratings = Column('n_ratings', Integer)
    rating_avg = Column('rating_avg', Float)
    rating_top1000 = Column('rating_top1000', Float)
    rating_us = Column('rating_us', Float)
    rating_row = Column('rating_row', Float)
    rating_M = Column('rating_M', Float)
    rating_F = Column('rating_F', Float)
    rating_0to18 = Column('rating_0to18', Float)
    rating_M_0to18 = Column('rating_M_0to18', Float)
    rating_F_0to18 = Column('rating_F_0to18', Float)
    rating_18to29 = Column('rating_18to29', Float)
    rating_M_18to29 = Column('rating_M_18to29', Float)
    rating_F_18to29 = Column('rating_F_18to29', Float)
    rating_29to45 = Column('rating_29to45', Float)
    rating_M_29to45 = Column('rating_M_29to45', Float)
    rating_F_29to45 = Column('rating_F_29to45', Float)
    rating_45to100 = Column('rating_45to100', Float)
    rating_M_45to100 = Column('rating_M_45to100', Float)
    rating_F_45to100 = Column('rating_F_45to100', Float)
    poster = Column(String)
    prediction = Column(Float)
