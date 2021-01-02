import logging
from io import StringIO

import pandas as pd
import requests

from crawler.models import db_connect
from helpers.printer import green


def refine_db():
    # Get data from IMDb database.
    engine = db_connect()
    logging.warning("Refining database...")
    imdb_series_df = pd.read_sql_query('SELECT * FROM imdb', con=engine, index_col="id")

    # Get a list of all the possible genres.
    genres = set("|".join(imdb_series_df["genres"].dropna().tolist()).split("|"))
    genres = list(filter(lambda x: len(x) > 0, genres))

    # Create and fill genre columns in the dataframe.
    for genre in genres:
        imdb_series_df[f"genre_{genre.lower()}"] = imdb_series_df['genres'].map(
            lambda x: int(genre in x), na_action='ignore')

    imdb_series_df = imdb_series_df.drop(columns=["genres"])

    # Export the dataframe to the database.
    logging.warning("Saving database to imdb table.")
    imdb_series_df.to_sql('imdb', engine, if_exists='replace')


def update_my_ratings():
    # Read movielens-ratings.csv from Google Drive.
    dwn_url = 'https://drive.google.com/uc?export=download&id='
    id_user_show = '1bJooXf85xiUV-I3a_kO3fJFlGNryTxef'
    movielens_ratings_df = pd.read_csv(StringIO(requests.get(dwn_url + id_user_show).text))

    logging.warning("Downloaded ratings from Google Drive.")
    
    movielens_ratings_df['rating'] = movielens_ratings_df['rating'].map(lambda x: x*2)
    movielens_ratings_df['average_rating'] = movielens_ratings_df['average_rating'].map(lambda x: round(x*2, 2))
    movielens_ratings_df.set_index('imdb_id', inplace=True)

    # Export the dataframe to the database.
    engine = db_connect()
    logging.warning("Saving database to my_ratings table.")
    movielens_ratings_df.to_sql('my_ratings', engine, if_exists='replace')
    logging.warning('Database updated!')


def show_predictions():
    # Get data from imdb database.
    engine = db_connect()
    imdb_series_df = pd.read_sql_query('SELECT * FROM imdb', con=engine, index_col="id")

    # Sort predictions.
    imdb_series_df.sort_values(by='prediction', ascending=False, inplace=True)

    # Create table to display in the page.
    stringa = '<table>' \
              '<tr><th>prediction</th><th>title</th></tr>'
    for i, row in imdb_series_df.iloc[0:10, :].iterrows():
        link = f'https://www.imdb.com/title/{row["url"]}/'
        stringa += f'<tr>' \
                   f'<td width=5%><b>{row["prediction"]:.2f}</b></td>' \
                   f'<td width=20%><b><a href="{link}" target="_blank">{row["name"]}</a></b></td>' \
                   f'</tr>'
    stringa += '</table>'

    return stringa
