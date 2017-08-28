import scrapy
from scrapy import Spider
from scrapy.selector import Selector
from stack.items import StackItem
import redis

r = redis.StrictRedis(host='localhost', port=6379, db=0)
url_prefix = "http://www.stackoverflow.com"
class StackSpider(Spider):
    name = "stack"
    allowed_domains = ["stackoverflow.com"]
    start_urls = [
        "http://stackoverflow.com/questions?pagesize=50&sort=newest",
    ]

    def parse(self, response):
        questions = Selector(response).xpath('//div[@class="summary"]/h3')
        for question in questions:
            item = StackItem()
            item['title'] = question.xpath('a[@class="question-hyperlink"]/text()').extract()[0]
            item['url'] = question.xpath('a[@class="question-hyperlink"]/@href').extract()[0]
            title = ''.join(item['title']).strip().encode('utf-8')
            url = url_prefix + ''.join(item['url']).strip().encode('utf-8')
            r.set(title, url)
            yield item
