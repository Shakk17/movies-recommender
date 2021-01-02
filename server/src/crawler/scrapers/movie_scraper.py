from bs4 import BeautifulSoup

from helpers.utility import strip_html_tags, transform_length


class MovieScraper:
    def __init__(self, body):
        self.html = BeautifulSoup(body, "lxml")

    def get_details(self, item):
        # OVERVIEW
        item['overview'] = self.html.find("div", {"class": 'inline canwrap'}).find('span').text
        # GENRES
        try:
            div = self.html.find_all("div", {"class": "see-more inline canwrap"})[1]
            genres = [a.text.strip() for a in div.find_all('a')]
            item["genres"] = "|".join(genres)
        except Exception:
            item["genres"] = None

        # YEAR
        item['year'] = self.html.find("span", {"id": "titleYear"}).find('a').text

        # LENGTH
        try:
            item["length"] = strip_html_tags(self.html.find("div", {"class": "subtext"}).find("time").text)
            item['length'] = transform_length(item["length"])
        except Exception:
            item["length"] = None

        # POPULARITY
        try:
            pop = self.html.find("div", {"class": "titleReviewBarSubItem"}).find("span").text
            pop = strip_html_tags(pop)
            item["popularity_rank"] = pop.split(" ")[0].replace(',', '')
        except Exception:
            item["popularity_rank"] = None

        # N_RATINGS
        item["n_ratings"] = self.html.find("div", {"class": "imdbRating"}).find('a').text.replace(',', '')

        # COUNTRY
        item['country'] = self.html.find('div', {'id': 'titleDetails'}).find_all('div')[1].find('a').text

        # POSTER
        poster_url = self.html.find('div', {'class': 'poster'}).find('img')['src'].split('@._V1_')[0]
        item['poster'] = poster_url + '@._V1_'

        return item

    def get_ratings_page(self):
        return self.html.find("div", {"class": "imdbRating"}).find('a')["href"]
