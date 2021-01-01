from bs4 import BeautifulSoup

from crawler.items import ImdbItem


class PageScraper:
    def __init__(self, body):
        self.html = BeautifulSoup(body, "lxml")

    def get_all_movies_items(self):
        divs = self.html.find_all("div", {"class": "lister-item-content"})
        progress = self.html.find("div", {"class": "desc"}).text.replace(',', '').replace('\n', '')
        tot = int(progress.split(" ")[2])
        infos = []
        for div in divs:
            tv_series_item = ImdbItem()
            tv_series_item["name"] = div.find('a').text
            tv_series_item["id"] = div.find('a')["href"].split('/')[2]

            tv_series_item["rating_avg"] = div.find("strong").text
            infos.append(tv_series_item)
        return infos, tot

    def get_next_page(self):
        try:
            url = self.html.find("a", {"class": "next-page"})["href"]
        except Exception:
            url = None
        return url



