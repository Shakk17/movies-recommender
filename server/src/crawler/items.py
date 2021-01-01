# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

from scrapy import Item, Field


class ImdbItem(Item):
    id = Field()
    name = Field()
    genres = Field()
    overview = Field()
    year = Field()
    length = Field()
    popularity_rank = Field()
    n_ratings = Field()
    rating_avg = Field()
    rating_top1000 = Field()
    rating_us = Field()
    rating_row = Field()
    rating_M = Field()
    rating_F = Field()
    rating_0to18 = Field()
    rating_M_0to18 = Field()
    rating_F_0to18 = Field()
    rating_18to29 = Field()
    rating_M_18to29 = Field()
    rating_F_18to29 = Field()
    rating_29to45 = Field()
    rating_M_29to45 = Field()
    rating_F_29to45 = Field()
    rating_45to100 = Field()
    rating_M_45to100 = Field()
    rating_F_45to100 = Field()
    poster = Field()
    prediction = Field()
