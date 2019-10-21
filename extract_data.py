import time, json, io, uuid
import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.http.request import Request
from scrapy.http import FormRequest
from scrapy.xlib.pydispatch import dispatcher
from scrapy.exceptions import CloseSpider
from hashlib import md5
from lxml import etree


class WODItem(scrapy.Item):
    wod_id = scrapy.Field()
    wod_title = scrapy.Field()
    load_score = scrapy.Field()
    time_score = scrapy.Field()
    amrap_score = scrapy.Field()
    workout_description = scrapy.Field()
    upload_timestamp = scrapy.Field()
    meta_datasource = scrapy.Field()


class WODScrapper(scrapy.Spider):
    name = 'wod-scraper'
    start_urls = ['https://wodwell.com']

    headers = {
        'User-Agent': 'Mr Robot'
    }

    custom_settings = {
        'LOG_LEVEL': 'INFO',
    }

    default_formdata = {
        'nf_ajax_query': "True",
        'sort': 'newest',
        'FuLmIeHjQoyqcz': 'imeFoLarXR3N9*',
        'NFufQjtR-mgPT': 'TNZVv@Y1fntJj',
        'KgMAYul': 'gAypIfE'
    }

    AJAX_endpoint = 'https://wodwell.com/wods'

    reached_limit = False

    feeds_tag_n = '1407,736,1611'

    num_ads = 0

    def __init__(self):
        dispatcher.connect(self.spider_closed, scrapy.signals.spider_closed)

    def start_requests(self):
        """ Start point for the scrapper 
        """
        self.logger.info('Entering main menu at url: {}'.format(self.start_urls[0]))
        yield Request(self.start_urls[0], headers=self.headers,
                      callback=self.get_url_all_wods)


    def get_url_all_wods(self, response):
        """ Function to parse response and extract URL for the 'All WODs (by Newest)'
        section on the webservice
        """
        root_url = response.url
        all_wods_url = response.xpath('//li/div/a/@href').get()
        url = '{}{}'.format(root_url, all_wods_url)
        url = url.replace('feeds=all', f'feeds={self.feeds_tag_n}')

        self.logger.info('All wods url composed at: {}'.format(url))
        yield Request(url, headers=self.headers,callback=self.parse)


    def parse(self, response):
        """ Function to extract maximum number of items in the webservice and
        yield AJAX requests to get all the WOD items from the server
        """
        max_results_count = response.xpath('//*[@id="content"]/div[1]/div[1]/h1/span[1]/text()').get()
        self.logger.info('Maximum number of WODS to parse: {}'.format(max_results_count.strip()))
        
        i = 0
        while not self.reached_limit:
            self.logger.info('Sending request number: {}'.format(str(i)))
            self.default_formdata['paged'] = str(i)
            self.default_formdata['feeds'] = self.feeds_tag_n
            yield FormRequest(self.AJAX_endpoint,
                              headers=self.headers,
                              formdata=self.default_formdata,
                              callback=self.parse_AJAX_resp,
                              meta={
                                  'paginator': str(i)
                              })
            i += 1
            # Used to slowdown code execution
            time.sleep(1.5)
    

    def parse_AJAX_resp(self, response):
        """ Parses AJAX response extracting all WOD items and yielding each
        one to Scrappy pipelines, final result is JSON file.
        """
        resp_success = json.loads(response.body)['success']
        paginator = response.meta['paginator']
        self.logger.info('Parsing response for paginator: {}'.format(paginator))
        if not resp_success:
            self.reached_limit = True
        else:
            num_wods_paginator = len(json.loads(response.body)['data']['wods'])
            self.logger.info('Number of items in paginator: {}'.format(num_wods_paginator))
            for wod in json.loads(response.body)['data']['wods']:
                if 'is_ad' in wod: self.num_ads += 1; continue
                if 'is_external_ad' in wod: self.num_ads += 1; continue
                wod_item = WODItem()
                self.get_workout_description(wod, wod_item)
                wod_item['wod_id'] = str(uuid.uuid5(uuid.NAMESPACE_URL, str(wod.get('wod_id'))))
                wod_item['wod_title'] = wod.get('title')
                wod_item['upload_timestamp'] = wod.get('date')
                wod_item['meta_datasource'] = 'wodwell'
                wod_item = self.extract_score_types(wod, wod_item)
                yield wod_item


    def get_workout_description(self, wod, wod_item):
        description = wod.get('workout')
        wod_item['workout_description'] = description.replace('<br/>', '\n')
        return wod_item


    def extract_score_types(self, wod, wod_item):
        score_types = wod.get('score_types')
        if score_types == '':
            wod_item['load_score'] = False
            wod_item['time_score'] = False
            wod_item['amrap_score'] = False
            return wod_item
        
        html_parser = etree.HTMLParser()
        tree = etree.parse(io.StringIO(score_types), html_parser)

        for elem in tree.xpath("//div[@class='wod-score-type']"): 
            if elem.text == 'For Load':
                wod_item['load_score'] = True
            elif elem.text == 'For Time':
                wod_item['time_score'] = True
            elif elem.text == 'For Rounds/Reps (AMRAP)':
                wod_item['amrap_score'] = True
            else:
                self.logger.info('New item!')
                import pdb; pdb.set_trace()
        return wod_item
    

    def spider_closed(self, spider):
        self.logger.info('Number of ads detected: {}'.format(self.num_ads))




if __name__ == "__main__":

    file_timestamp = time.time()
    process = CrawlerProcess({
        'FEED_URI': f'{file_timestamp}-WODS.json',
        'FEED_FORMAT': 'jsonlines'
    })
    process.crawl(WODScrapper)
    process.start()
