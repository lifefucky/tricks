# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy.loader.processors import MapCompose, TakeFirst, Identity

def toFloat(number):
    try:
        return float(number.strip())
    except:
        return number

class LeroymerlinItem(scrapy.Item):
    # define the fields for your item here like:
    _id = scrapy.Field()
    name = scrapy.Field(output_processor=TakeFirst())
    price = scrapy.Field(outputprocessor=TakeFirst(), inputprocessor=MapCompose(toFloat))
    link = scrapy.Field()
    images = scrapy.Field(output_processor=Identity())
    parameters = scrapy.Field()
    pass

