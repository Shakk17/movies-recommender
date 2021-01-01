import logging

import pandas as pd
import xgboost as xgb
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer, KNNImputer
from sklearn.metrics import mean_absolute_error
from sklearn.model_selection import train_test_split, cross_val_score, GridSearchCV
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import OrdinalEncoder, StandardScaler, MinMaxScaler

from crawler.models import db_connect

logging.basicConfig(format='%(asctime)s - %(message)s')


def train_model():
    # Import datasets.
    engine = db_connect()
    imdb_df = pd.read_sql_query('SELECT * FROM imdb', con=engine, index_col="id")
    my_ratings_df = pd.read_sql_query('SELECT * FROM my_ratings', con=engine, index_col="imdb_id")

    # Merge datasets together.
    movies_df = pd.merge(imdb_df, my_ratings_df, how="left", left_index=True, right_index=True)

    # Drop prediction feature.
    movies_df.drop('prediction', axis=1, inplace=True)

    # Save a copy to display results after prediction.
    main_df = movies_df.copy()

    # Split training set and test set.
    rated_df = movies_df.dropna(axis=0, subset=["rating"])

    unrated_df = movies_df[movies_df["rating"].isna()].drop("rating", axis=1)

    X = rated_df.drop(["rating"], axis=1)
    y = rated_df["rating"]

    X_train, X_test, y_train, y_test = train_test_split(X, y, train_size=0.8, random_state=17)

    preprocessor = create_preprocessor(movies_df)

    model = make_pipeline(
        preprocessor,
        xgb.XGBRegressor(random_state=17))

    # Tune parameters.
    param_grid = {
        'xgbregressor__n_estimators': [40],
        'xgbregressor__max_depth': [7],
        'xgbregressor__min_child_weight': [8],
        'xgbregressor__eta': [.1],
        'xgbregressor__eval_metric': ['mae']
    }

    grid_clf = GridSearchCV(model, param_grid, cv=10, n_jobs=4, verbose=1)
    grid_clf.fit(X_train, y_train)

    logging.warning(grid_clf.best_params_)

    model = grid_clf.best_estimator_

    # Calculate Mean Absolute Error over training set.
    scores = cross_val_score(model, X_train, y_train, scoring="neg_mean_absolute_error", cv=10)
    logging.warning(f"MAE on training set: {-scores.mean():.2f} (+/- {(scores.std() * 2):.2f})")

    # Predict results for test set.
    predictions = model.predict(X_test)

    # Display results comparing them to real personal ratings.
    valid_results = pd.DataFrame(data=dict(prediction=predictions, real=y_test.to_list(),
                                           difference=predictions - y_test.to_list()),
                                 index=main_df.loc[y_test.index, "name"])
    logging.warning(valid_results.sort_values(by="difference", ascending=False).round(decimals=2))

    logging.warning(f"MAE on test set: {mean_absolute_error(y_test, predictions):.2f}")

    predictions = model.predict(unrated_df)

    # Display best tv series to watch.
    predictions_df = main_df.copy().dropna(how='all')
    predictions_df["prediction"] = pd.Series(data=predictions, index=unrated_df.index)

    # Sort and round numbers.
    predictions_df = predictions_df.sort_values(by="prediction", ascending=False)[
        ["name", "prediction"]].round(decimals=2)

    # Save predictions to tvdb table.
    imdb_df['prediction'] = predictions_df["prediction"]
    # Export the dataframe to the database.
    logging.warning("Saving predictions to imdb table.")
    imdb_df.to_sql('imdb', engine, if_exists='replace')
    logging.warning('Database updated!')


def create_preprocessor(movies_df):
    year_pipe = make_pipeline(
        SimpleImputer(strategy="constant", fill_value=movies_df["year"].max()),
        MinMaxScaler())

    genre_pipe = make_pipeline(
        SimpleImputer(strategy="constant", fill_value=0))

    avg_ratings_pipe = make_pipeline(
        KNNImputer(n_neighbors=5, weights="uniform"),
        StandardScaler())

    popularity_pipe = make_pipeline(
        SimpleImputer(strategy="constant", fill_value=movies_df["popularity_rank"].max(), add_indicator=True),
        MinMaxScaler())

    knn_pipe = make_pipeline(
        KNNImputer(n_neighbors=3, weights="distance"),
        MinMaxScaler()
    )

    year_cat = ["year"]
    genre_cat = [name for name in movies_df.columns if name.startswith("genre")]
    avg_ratings_cat = [name for name in movies_df.columns if name.startswith("rating_")]
    popularity_cat = ["popularity_rank"]
    knn_cat = ['n_ratings', 'length']

    transformers = [
        ('year', year_pipe, year_cat),
        ('genres', genre_pipe, genre_cat),
        ('avg_ratings', avg_ratings_pipe, avg_ratings_cat),
        ('popularity', popularity_pipe, popularity_cat),
        ('knn', knn_pipe, knn_cat)
    ]

    preprocessor = ColumnTransformer(transformers, remainder='drop')

    return preprocessor
