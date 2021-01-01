# This package will contain the spiders of your Scrapy project
#
# Please refer to the documentation for information on how to create and manage
# your spiders.

import scrapy
from time import time

from crawler.scrapers.ratings_scraper import RatingsScraper
from crawler.scrapers.page_scraper import PageScraper
from crawler.scrapers.movie_scraper import MovieScraper


class ImdbSpider(scrapy.Spider):
    name = "imdb_spider"

    tot_items = None
    items = 0
    time = time()

    num_votes = 50000
    release_date = 1970
    min_rating = 7.2

    start_urls = [f"https://www.imdb.com/search/title/?count=100&num_votes={num_votes},&release_date={release_date},"
                  f"&title_type=movie&user_rating={min_rating},"]

    def parse(self, response):
        scraper = PageScraper(response.body)

        items, self.tot_items = scraper.get_all_movies_items()
        for item in items:
            url = f"/title/{item['id']}/"
            yield response.follow(url, callback=self.parse_movie, meta={"movie_item": item})

        next_page = scraper.get_next_page()
        if next_page is not None:
            next_page = response.urljoin(next_page)
            yield scrapy.Request(next_page, callback=self.parse)

    def parse_movie(self, response):
        scraper = MovieScraper(response.body)
        movie_item = scraper.get_details(item=response.meta["movie_item"])

        ratings_page = scraper.get_ratings_page()
        ratings_page = response.urljoin(ratings_page)

        yield response.follow(ratings_page, callback=self.parse_ratings, meta={"movie_item": movie_item})

    def parse_ratings(self, response):
        scraper = RatingsScraper(response.body)
        movie_item = scraper.get_all_ratings(item=response.meta["movie_item"])
        yield movie_item
