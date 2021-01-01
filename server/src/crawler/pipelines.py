from sqlalchemy.orm import sessionmaker

from crawler.models import db_connect, create_table, Movie
from helpers.printer import green, blue

from time import time
import logging


class ImdbCrawlerPipeline:
    @classmethod
    def from_crawler(cls, crawler):
        # Instantiate the pipeline with your table
        return cls()

    def __init__(self):
        """
        Initializes database connection and sessionmaker
        Creates tables
        """
        engine = db_connect()
        create_table(engine)
        self.Session = sessionmaker(bind=engine)

    def process_item(self, item, spider):
        """
        This method is called for every item pipeline component
        """
        session = self.Session()

        movie = Movie()
        movie.id = item["id"]
        movie.url = item['url']
        movie.name = item["name"]
        movie.genres = item["genres"]
        movie.overview = item['overview']
        movie.year = item["year"]
        movie.length = item["length"]
        movie.popularity_rank = item["popularity_rank"]
        movie.n_ratings = item["n_ratings"]
        movie.rating_avg = item["rating_avg"]
        movie.rating_top1000 = item["rating_top1000"]
        movie.rating_us = item["rating_us"]
        movie.rating_row = item["rating_row"]
        movie.rating_M = item["rating_M"]
        movie.rating_F = item["rating_F"]
        movie.rating_0to18 = item["rating_0to18"]
        movie.rating_M_0to18 = item["rating_M_0to18"]
        movie.rating_F_0to18 = item["rating_F_0to18"]
        movie.rating_18to29 = item["rating_18to29"]
        movie.rating_M_18to29 = item["rating_M_18to29"]
        movie.rating_F_18to29 = item["rating_F_18to29"]
        movie.rating_29to45 = item["rating_29to45"]
        movie.rating_M_29to45 = item["rating_M_29to45"]
        movie.rating_F_29to45 = item["rating_F_29to45"]
        movie.rating_45to100 = item["rating_45to100"]
        movie.rating_M_45to100 = item["rating_M_45to100"]
        movie.rating_F_45to100 = item["rating_F_45to100"]
        movie.poster = item['poster']
        movie.prediction = None

        try:
            session.add(movie)
            session.commit()
            spider.items += 1
            per = spider.items / spider.tot_items * 100
            per_string = green(f"[{per:.1f}%]")
            logging.warning(f"{per_string} {spider.items} {movie.name}")
            # Print speed every 100 items.
            if spider.items % 50 == 0:
                speed = 50 / (time() - spider.time)
                logging.warning(blue(f"[SPEED] {speed:.2f} e/s"))
                spider.time = time()

        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

        return item
