import scrapy
from scrapy import Spider
from scrapy.selector import Selector
from stack.items import StackItem
from confluent_kafka import Producer
import kafka
from blist import sortedlist

p = Producer({'bootstrap.servers': 'localhost:9092'})
topic_prefix = "stackoverflow"
url_prefix = "http://www.stackoverflow.com"
consumer = kafka.KafkaConsumer('*', group_id='test', bootstrap_servers=['localhost:9092'])

def getPosition(hash):
    hash_ring = sortedlist([])
    topics = consumer.topics()
    print topics
    for topic in topics:
        print "topic is " + topic
        position = topic[len(topic_prefix):]
        if position:
            print "adding position " + position + " in the hash ring."
            hash_ring.add(position)
    if len(hash_ring) is 0:
        print "No shard worker in the hash ring."
        return -1
    print str(len(hash_ring)) + " nodes in the hash ring"
    for i in range(len(hash_ring)):
        pos = hash_ring[i]
        if pos > hash:
            return pos
    print "Gonna return the first node in the hash ring"
    return hash_ring[0]


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
            hashValue = hash(title)
            if (hashValue < 0):
                hashValue = abs(hashValue)
            pos = getPosition(hashValue)
            topic = topic_prefix + pos
            p.produce(topic, key=title, value=url)
            p.flush(30)
            yield item
