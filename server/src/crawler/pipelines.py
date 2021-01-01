from sqlalchemy.orm import sessionmaker

from crawler.models import db_connect, create_table, TvSeries
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

        tv_series = TvSeries()
        tv_series.id = item["id"]
        tv_series.name = item["name"]
        tv_series.genres = item["genres"]
        tv_series.overview = item['overview']
        tv_series.year = item["year"]
        tv_series.length = item["length"]
        tv_series.popularity_rank = item["popularity_rank"]
        tv_series.n_ratings = item["n_ratings"]
        tv_series.rating_avg = item["rating_avg"]
        tv_series.rating_top1000 = item["rating_top1000"]
        tv_series.rating_us = item["rating_us"]
        tv_series.rating_row = item["rating_row"]
        tv_series.rating_M = item["rating_M"]
        tv_series.rating_F = item["rating_F"]
        tv_series.rating_0to18 = item["rating_0to18"]
        tv_series.rating_M_0to18 = item["rating_M_0to18"]
        tv_series.rating_F_0to18 = item["rating_F_0to18"]
        tv_series.rating_18to29 = item["rating_18to29"]
        tv_series.rating_M_18to29 = item["rating_M_18to29"]
        tv_series.rating_F_18to29 = item["rating_F_18to29"]
        tv_series.rating_29to45 = item["rating_29to45"]
        tv_series.rating_M_29to45 = item["rating_M_29to45"]
        tv_series.rating_F_29to45 = item["rating_F_29to45"]
        tv_series.rating_45to100 = item["rating_45to100"]
        tv_series.rating_M_45to100 = item["rating_M_45to100"]
        tv_series.rating_F_45to100 = item["rating_F_45to100"]
        tv_series.poster = item['poster']
        tv_series.prediction = None

        try:
            session.add(tv_series)
            session.commit()
            spider.items += 1
            per = spider.items / spider.tot_items * 100
            per_string = green(f"[{per:.1f}%]")
            logging.warning(f"{per_string} {spider.items} {tv_series.name}")
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
