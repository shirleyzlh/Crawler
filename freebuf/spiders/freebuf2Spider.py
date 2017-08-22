import scrapy
from freebuf.items import FreebufItem
import time
from scrapy.crawler import CrawlerProcess

class freebuf2Spider(scrapy.Spider):
    name ='freebuf'
    allowed_domains = []

    start_urls = ["http://www.freebuf.com/"]

    def parse(self, response):

        for link in response.xpath("//div[contains(@class, 'news_inner news-list')]/div/a/@href").extract():
            yield scrapy.Request(link, callback=self.parse_next)
        next_url = response.xpath("//div[@class='news-more']/a/@href").extract()
        if next_url:
            yield scrapy.Request(next_url[0],callback=self.parse)

    def parse_next(self,response):
        item = FreebufItem()
        item['title'] = response.xpath("//h2/text()").extract()
        item['url'] = response.url
        item['date'] = response.xpath("//div[@class='property']/span[@class='time']/text()").extract()
        item['tags'] = response.xpath("//span[@class='tags']/a/text()").extract()
        with open('test_file', 'a') as f:
            title = item['title'][0].encode('utf-8')
            url = item['url'][0].encode('utf-8')
            f.write(title)
            f.write(url)
        yield item
