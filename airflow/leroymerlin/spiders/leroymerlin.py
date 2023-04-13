import scrapy
from scrapy.http import HtmlResponse
from leroymerlin.items import LeroymerlinItem
from scrapy.loader import ItemLoader


class LeroymerlinSpider(scrapy.Spider):
    name = 'leroymerlin'
    allowed_domains = ['leroymerlin.ru']

    def __init__(self, query):
        self.start_urls = [f'https://leroymerlin.ru/catalogue/stroymaterialy/']


    def parse(self, response: HtmlResponse):
        links = response.xpath("//product-card/@data-product-url").extract()
        current_page = int(response.xpath("//uc-pagination/@current").extract_first())
        total_page = int(response.xpath("//uc-pagination/@total").extract_first())
        main_mask = 'https://leroymerlin.ru'

        for link in links:
            yield response.follow(main_mask + link, callback=self.vacancy_parse)
        if total_page-current_page:
            yield response.follow(self.start_urls[0] + '?page='+str(current_page+1), callback=self.parse)

    def vacancy_parse(self, response: HtmlResponse):
        loader = ItemLoader(item=LeroymerlinItem(), response=response)
        loader.add_xpath('name', "//h1/text()")
        loader.add_xpath('price', "//div[@class='product-content']/div/@data-product-price")
        loader.add_value('link', response.url)
        loader.add_xpath('images', "//picture[contains(@id,'picture-box-id-generated-')]/source[contains(@media,'1024px')]/@srcset")
        parameters = {}
        for line in response.xpath("//div[@class='def-list__group']").extract():
            key = line.xpath("/dt/text()").extract_first()
            value = line.xpath("/dd/text()").extract_first()
            parameters[key] = value
        loader.add_value('parameters', parameters)
        yield loader.load_item()