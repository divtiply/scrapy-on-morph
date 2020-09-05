import scrapy


class MorphSpider(scrapy.Spider):
    name = 'morph'
    allowed_domains = ['morph.io']
    start_urls = ['https://morph.io/users']

    def parse(self, response):
        for element in response.css('.list-group-item'):
            username = element.css('img::attr(alt)').get()
            yield dict(username=username)
