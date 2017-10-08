import scrapy
from scrapy import Spider
from scrapy.selector import Selector
from stack.items import StackItem
from confluent_kafka import Producer
from blist import sortedlist
from kazoo.client import KazooClient
from random import randint

zk = KazooClient(hosts='127.0.0.1:2181', read_only=True)
zk.start()
p = Producer({'bootstrap.servers': 'localhost:9092'})
topic_prefix = "stackoverflow"
url_prefix = "http://www.stackoverflow.com"

def init_hash_ring():
    hash_ring = sortedlist([])
    if zk.exists("/ContinousQuery/hashRing") is None:
        print "No nodes are in the hash ring"
        return hash_ring
    print "Hash ring exists"
    topics = zk.get_children("/ContinousQuery/hashRing")
    print topics
    for topic in topics:
        print "topic is " + topic
        hash_ring.add(topic)
    print str(len(hash_ring)) + " nodes in the hash ring"
    return hash_ring

hash_ring = init_hash_ring()

def getPosition(hash):
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
            if len(hash_ring) == 0:
                print "No nodes are in the hash ring"
                return
            hashValue = hash(title)
            if (hashValue < 0):
                hashValue = abs(hashValue)
            print "Hash value is " + str(hashValue)
            hashValue = hashValue % 128
            pos = getPosition(hashValue)
            topic = topic_prefix + pos
            print "producing entry to topic " + topic
            p.produce(topic, key=title, value=url)
            p.flush(30)
            yield item
