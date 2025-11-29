import scrapy
from urllib.parse import urlparse, parse_qs
import re, os
from ..items import RaceItem, HorseResultItem, SectimeItem, IncidentItem
import json
import pandas as pd
from datetime import datetime

base_url = "https://racing.hkjc.com/racing/information/English/Racing/LocalResults.aspx?RaceDate={date}&Racecourse={racecourse}&RaceNo={race_no}"


class HkjcSpider(scrapy.Spider):
    name = "race_results"
    allowed_domains = ["racing.hkjc.com"]

    season = "2025-26"
    race_dict = pd.read_csv(f"seasons_race_day//{season}.txt").to_dict(orient="records")
    data_folder_path = os.path.join("data", season, datetime.now().strftime("%Y-%m-%d_%H-%M-%S"))

    start_urls = [base_url.format(date=row["date"], racecourse=row["racecourse"], race_no=race_no) for row in race_dict for race_no in range(1, row["max_race"] + 1)]

    custom_settings = {
        "FEEDS": {
            f"{data_folder_path}/races.csv": {
                "format": "csv",
                "item_classes": ["hkjc_scraper.items.RaceItem"],
                "encoding": "utf8",
                "overwrite": True,
            },
            f"{data_folder_path}/results.csv": {
                "format": "csv",
                "item_classes": ["hkjc_scraper.items.HorseResultItem"],
                "encoding": "utf8",
                "overwrite": True,
            },
            f"{data_folder_path}/sectime.csv": {
                "format": "csv",
                "item_classes": ["hkjc_scraper.items.SectimeItem"],
                "encoding": "utf8",
                "overwrite": True,
            },
            f"{data_folder_path}/incident.csv": {
                "format": "csv",
                "item_classes": ["hkjc_scraper.items.IncidentItem"],
                "encoding": "utf8",
                "overwrite": True,
            },
        }
    }

    def parse(self, response):
        race_item = self.parse_race_item(response)
        yield race_item

        for horse_item in self.parse_horse_items(response):
            yield horse_item

        for incident_item in self.parse_incident_item(response):
            yield incident_item

        sectime_page_url = response.css("div.raceMeeting_select p.sectional_time_btn.f_clear a::attr(href)").get()
        if sectime_page_url:
            yield response.follow(sectime_page_url, callback=self.parse_sectime_item)

    # CSS Selectors
    SELECTORS = {
        "date_racecourse": "span.f_fl.f_fs13::text",
        "race_info": "div.race_tab table thead tr td:nth-child(1)::text",
        "class_dist_rating": "div.race_tab table tbody tr:nth-child(2) td:nth-child(1)::text",
        "race_name": "div.race_tab table tbody tr:nth-child(3) td:nth-child(1)::text",
        "going": "div.race_tab table tbody tr:nth-child(2) td:nth-child(3)::text",
        "prize": "div.race_tab table tbody tr:nth-child(4) td:nth-child(1)::text",
        "track_course": "div.race_tab table tbody tr:nth-child(3) td:nth-child(3)::text",
        "sectional_times": "div.race_tab table tbody tr:nth-child(5) td:nth-child(n+3)",
        "horse_table": "table.f_tac.table_bd.draggable",
    }

    # Regex Patterns
    PATTERNS = {
        "date_racecourse": r"(\d{2}/\d{2}/\d{4})\s+([A-Za-z ]+)$",
        "race_info": r"(\d+)\s+\((\d+)\)",
        "class_dist_rating": r"(.*) - (\d+)M - \(([\d\-]+)\)",
        "class_dist_no_rating": r"(.*) - (\d+)M",
        "track_course": r"(.*) - \"(.*)\"",
    }

    def parse_race_item(self, response):
        race_item = RaceItem()

        race_item["url"] = response.url
        # Extract date and racecourse
        race_item["date"], race_item["racecourse"] = response.css(self.SELECTORS["date_racecourse"]).re(self.PATTERNS["date_racecourse"])
        race_item["race_number"], race_item["race_index"] = response.css(self.SELECTORS["race_info"]).re(self.PATTERNS["race_info"])

        # Extract class, distance, and optional rating

        class_dist_rating_text = response.css(self.SELECTORS["class_dist_rating"]).get()
        search = re.search(self.PATTERNS["class_dist_rating"], class_dist_rating_text)
        if search:
            race_item["class_name"], race_item["distance"], race_item["rating"] = search.groups()
        else:
            search = re.search(self.PATTERNS["class_dist_no_rating"], class_dist_rating_text)
            race_item["class_name"], race_item["distance"] = search.groups()

        # Extract race name, going, and prize
        race_item["name"] = response.css(self.SELECTORS["race_name"]).get(default="").strip()
        race_item["going"] = response.css(self.SELECTORS["going"]).get(default="").strip()
        race_item["prize"] = response.css(self.SELECTORS["prize"]).get(default="").strip()

        # Extract race name, going, and prize
        track_course_text = response.css(self.SELECTORS["track_course"]).get(default="")
        search = re.search(self.PATTERNS["track_course"], track_course_text)
        if search:
            race_item["track"], race_item["course"] = search.groups()
        else:
            race_item["track"] = track_course_text

        # Extract sectional times
        sectimes = response.css(self.SELECTORS["sectional_times"])
        for i, sec in enumerate(sectimes, start=1):
            race_item[f"sec{i}_time"] = sec.css("td::text").get().strip()

        return race_item

    def parse_horse_items(self, response):

        table = response.css("table.f_tac.table_bd.draggable")

        column_count = len(table.css("tr")[0].css("td"))
        rows = table.css("tr")[1:]  # Skip header row

        date = parse_qs(urlparse(response.url).query).get("RaceDate", [""])[0]
        race_no, race_index = re.search(self.PATTERNS["race_info"], response.css(self.SELECTORS["race_info"]).get()).groups()

        for row in rows:
            # Initialize HorseResultItem

            item = HorseResultItem()
            item["date"] = date
            item["race_number"] = race_no
            item["race_index"] = race_index
            item["position"] = row.css("td:nth-child(1)::text").get(default="").strip()
            item["horse_number"] = row.css("td:nth-child(2)::text").get(default="").strip()
            item["horse_name"] = row.css("td:nth-child(3) a::text").get(default="").strip()
            item["horse_id"] = parse_qs(urlparse(row.css("td:nth-child(3) a::attr(href)").get(default="")).query).get("HorseId", [""])[0]

            # jockey handling
            if row.css("td:nth-child(4) a"):
                item["jockey"] = row.css("td:nth-child(4) a::text").get(default="").strip()
                item["jockey_id"] = parse_qs(urlparse(row.css("td:nth-child(4) a::attr(href)").get(default="")).query).get("JockeyId", [""])[0]
            else:
                item["jockey"] = row.css("td:nth-child(4)::text").get(default="").strip()

            # trainer handling
            if row.css("td:nth-child(5) a"):
                item["trainer"] = row.css("td:nth-child(5) a::text").get(default="").strip()
                item["trainer_id"] = parse_qs(urlparse(row.css("td:nth-child(5) a::attr(href)").get(default="")).query).get("TrainerId", [""])[0]
            else:
                item["trainer"] = row.css("td:nth-child(5)::text").get(default="").strip()

            item["actual_weight"] = row.css("td:nth-child(6)::text").get(default="").strip()
            item["declar_horse_wt"] = row.css("td:nth-child(7)::text").get(default="").strip()
            item["draw"] = row.css("td:nth-child(8)::text").get(default="").strip()
            item["lbw"] = row.css("td:nth-child(9)::text").get(default="").strip()

            if column_count > 11:
                item["running_position"] = " ".join(pos.strip() for pos in row.css("td:nth-child(10) div div::text").getall() if pos.strip())
                item["finish_time"] = row.css("td:nth-child(11)::text").get(default="").strip()
                item["win_odds"] = row.css("td:nth-child(12)::text").get(default="").strip()
            else:
                item["finish_time"] = row.css("td:nth-child(10)::text").get(default="").strip()
                item["win_odds"] = row.css("td:nth-child(11)::text").get(default="").strip()

            yield item

    def parse_sectime_item(self, response):
        table = response.css("table.table_bd.f_tac.race_table tbody")
        rows = table.css("tr")

        date = parse_qs(urlparse(response.url).query).get("RaceDate", [""])[0]
        race_no = parse_qs(urlparse(response.url).query).get("RaceNo", [""])[0]

        for row in rows:
            # Initialize SectimeItem
            for section in range(6):
                item = SectimeItem()
                item["date"] = date
                item["race_number"] = race_no
                item["position_final"] = row.css("td:nth-child(1)::text").get(default="").strip()
                item["horse_number"] = row.css("td:nth-child(2)::text").get(default="").strip()
                item["horse_name"], item["horse_code"] = row.css("td:nth-child(3) a::text").re(r"^(.*?)\s*\((.*?)\)$")
                item["horse_id"] = parse_qs(urlparse(row.css("td:nth-child(3) a::attr(href)").get(default="")).query).get("HorseId", [""])[0]
                item["section"] = section + 1
                item["position"] = row.css(f"td:nth-child({section+4}) span.f_fl::text").get(default="").strip()

                if not item["position"]:
                    continue

                item["lbw"] = row.css(f"td:nth-child({section+4}) i::text").get(default="").strip()
                item["time"] = row.css(f"td:nth-child({section+4}) p::text").get(default="").strip()
                item["subtime1"], item["subtime2"] = row.css(f"td:nth-child({section+4}) span.color_blue2 span::text").getall() or (None, None)

                yield item

    def parse_incident_item(self, response):
        table = response.css("table.f_tac.table_bd tbody.f_fs12.fontFam")
        rows = table.css("tr")

        date = parse_qs(urlparse(response.url).query).get("RaceDate", [""])[0]
        race_no = parse_qs(urlparse(response.url).query).get("RaceNo", [""])[0]

        for row in rows:
            # Initialize SectimeItem
            item = IncidentItem()
            item["date"] = date
            item["race_number"] = race_no
            item["position_final"] = row.css("td:nth-child(1)::text").get(default="").strip()
            item["horse_number"] = row.css("td:nth-child(2)::text").get(default="").strip()
            item["horse_code"] = row.css("td:nth-child(3)::text").re(r"\(([^)]+)\)")[0]
            item["horse_name"] = row.css("td:nth-child(3) a::text").get(default="").strip()
            item["horse_id"] = parse_qs(urlparse(row.css("td:nth-child(3) a::attr(href)").get(default="")).query).get("HorseId", [""])[0]
            item["incident"] = row.css("td:nth-child(4)::text").get(default="").strip()
            yield item
