# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class AppliedPdpItem(scrapy.Item):
    index_id = scrapy.Field()
    url = scrapy.Field()
    mpn = scrapy.Field()
    product_name = scrapy.Field()
    product_code = scrapy.Field()
    product_description = scrapy.Field()
    product_image = scrapy.Field()
    product_price = scrapy.Field()
    product_specs = scrapy.Field()
    categories = scrapy.Field()
    availability = scrapy.Field()
