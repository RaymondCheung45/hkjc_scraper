# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
from datetime import datetime

import scrapy.item


class RaceItem(scrapy.Item):
    date = scrapy.Field(serializer=lambda x: datetime.strptime(x, "%d/%m/%Y").strftime("%Y-%m-%d"))
    racecourse = scrapy.Field()
    race_number = scrapy.Field(serializer=lambda x: int(x))
    race_index = scrapy.Field(serializer=lambda x: int(x))
    class_name = scrapy.Field()
    distance = scrapy.Field(serializer=lambda x: int(x))
    rating = scrapy.Field()
    name = scrapy.Field()
    prize = scrapy.Field(serializer=lambda x: int(x.replace("HK$", "").replace(",", "")))
    going = scrapy.Field()
    track = scrapy.Field()
    course = scrapy.Field()
    sec1_time = scrapy.Field()
    sec2_time = scrapy.Field()
    sec3_time = scrapy.Field()
    sec4_time = scrapy.Field()
    sec5_time = scrapy.Field()
    sec6_time = scrapy.Field()
    url = scrapy.Field()


def parse_finish_time(time_str):
    if not time_str:
        return None
    return round(float(time_str.split(":")[0]) * 60 + float(time_str.split(":")[1]), 2)


class HorseResultItem(scrapy.Item):
    date = scrapy.Field(serializer=lambda x: datetime.strptime(x, "%Y/%m/%d").strftime("%Y-%m-%d"))
    race_number = scrapy.Field()
    race_index = scrapy.Field()
    position = scrapy.Field()
    horse_number = scrapy.Field()
    horse_name = scrapy.Field()
    horse_id = scrapy.Field()
    jockey = scrapy.Field()
    jockey_id = scrapy.Field()
    trainer = scrapy.Field()
    trainer_id = scrapy.Field()
    actual_weight = scrapy.Field()
    declar_horse_wt = scrapy.Field()
    draw = scrapy.Field()
    lbw = scrapy.Field()
    running_position = scrapy.Field()
    finish_time = scrapy.Field(serializer=parse_finish_time)
    win_odds = scrapy.Field()


class SectimeItem(scrapy.Item):
    date = scrapy.Field(serializer=lambda x: datetime.strptime(x, "%d/%m/%Y").strftime("%Y-%m-%d"))
    race_number = scrapy.Field()
    position_final = scrapy.Field()
    horse_number = scrapy.Field()
    horse_name = scrapy.Field()
    horse_id = scrapy.Field()
    horse_code = scrapy.Field()
    section = scrapy.Field()
    position = scrapy.Field()
    lbw = scrapy.Field()
    time = scrapy.Field()
    subtime1 = scrapy.Field()
    subtime2 = scrapy.Field()


class IncidentItem(scrapy.Item):
    date = scrapy.Field(serializer=lambda x: datetime.strptime(x, "%Y/%m/%d").strftime("%Y-%m-%d"))
    race_number = scrapy.Field()
    position_final = scrapy.Field()
    horse_number = scrapy.Field()
    horse_name = scrapy.Field()
    horse_id = scrapy.Field()
    horse_code = scrapy.Field()
    incident = scrapy.Field()
