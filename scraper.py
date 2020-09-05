# scrapy morph.io runner
# https://morph.io/documentation
# https://docs.scrapy.org/en/latest/topics/practices.html#run-from-script

from os import environ, getenv

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings


def main():
    spider = getenv('MORPH_SPIDER')
    spider_args = env_to_dict('MORPH_SPIDER_ARGS')
    spider_settings = env_to_dict('MORPH_SPIDER_SETTINGS')
    environ.update(env_to_dict('MORPH_DOTENV'))

    settings = get_project_settings()
    settings.setdict(spider_settings, priority='cmdline')
    process = CrawlerProcess(settings)
    if spider is None:
        spider = next(iter(process.spider_loader.list()))
    process.crawl(spider, **spider_args)
    process.start()


def env_to_dict(key):
    envstr = getenv(key, '')
    return dict(
        line.split('=', 1)
        for line in envstr.splitlines()
        if line and not line.startswith('#')
    )


if __name__ == '__main__':
    main()
