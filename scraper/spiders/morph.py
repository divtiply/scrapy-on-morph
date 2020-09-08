import scrapy


class MorphSpider(scrapy.Spider):
    name = 'morph'
    allowed_domains = ['morph.io']
    start_urls = ['https://morph.io/users']

    def parse(self, response):
        pagination = response.css('.pagination .page a:not([href^=javascript])')
        yield from response.follow_all(pagination)
        for element in response.css('.list-group-item'):
            item = {}
            item['username'] = element.css('img::attr(alt)').get()
            name = element.css('h2::text').get()
            nickname = element.css('.owner-nickname::text').get()
            item['fullname'] = name.strip() if nickname else None
            item['organization'] = element.css('h2 ~ div::text').get()
            item['avatar'] = element.css('img::attr(src)').get()
            yield {k: v for k, v in item.items() if v is not None}
