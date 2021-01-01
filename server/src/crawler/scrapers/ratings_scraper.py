from bs4 import BeautifulSoup


class RatingsScraper:
    def __init__(self, body):
        self.html = BeautifulSoup(body, "lxml")

    def get_all_ratings(self, item):
        divs = self.html.find_all("div", {"class": "bigcell"})
        item["rating_avg"] = divs[0].text
        item["rating_top1000"] = divs[15].text
        item["rating_us"] = divs[16].text
        item["rating_row"] = divs[17].text
        item["rating_M"] = divs[5].text
        item["rating_F"] = divs[10].text
        item["rating_0to18"] = divs[1].text
        item["rating_M_0to18"] = divs[6].text
        item["rating_F_0to18"] = divs[11].text
        item["rating_18to29"] = divs[2].text
        item["rating_M_18to29"] = divs[7].text
        item["rating_F_18to29"] = divs[12].text
        item["rating_29to45"] = divs[3].text
        item["rating_M_29to45"] = divs[8].text
        item["rating_F_29to45"] = divs[13].text
        item["rating_45to100"] = divs[4].text
        item["rating_M_45to100"] = divs[9].text
        item["rating_F_45to100"] = divs[14].text

        # Check for '-' values.
        for key, value in item.items():
            if value == '-':
                item[key] = None
        return item
