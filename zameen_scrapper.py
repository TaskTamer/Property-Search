from pipelines import DataPipeline
# from plots_pipelines import PlotsPipeline
# packages
import math
import logging
import scrapy
from scrapy.crawler import CrawlerProcess
import json
import time
import os
import shutil

# Check if 'data.db' exists in 'main' directory
if os.path.exists('data.db'):
    # Remove 'data.db' from 'main' directory
    os.remove('data.db')
    print("Removed 'data.db' from 'main' directory")

homes = 0
total_pages = 0


class ZameenScraper(scrapy.Spider):

    features = []
    property = ''

    # crawler's entry point

    def start_requests(self):

        url = self.base_url + '1.html'

        yield scrapy.Request(
            url=url,

            headers={
                'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36'
            },
            callback=self.parse_houses_count,
        )

    def parse_houses_count(self, response):
        global total_pages
        # Extract the number of houses from the page
        houses_element = response.css("h1._2aa3d08d")
        houses_text = houses_element.css('::text').get()
        number_of_houses = int(''.join(filter(str.isdigit, houses_text)))

        # Calculate the number of pages
        no_of_pages = math.ceil(number_of_houses / 25)
        total_pages += no_of_pages
        print(no_of_pages)

        # Generate and yield the next page URLs
        for page in range(1, no_of_pages + 2):
            next_page = self.base_url + str(page) + '.html'
            # print(next_page)
            yield scrapy.Request(
                url=next_page,
                headers={
                    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36'
                },
                callback=self.parse,
            )

    # parse property cards

    def parse(self, response):
        global homes
        self.features = []
        for card in response.css('li[role="article"]'):
            if self.property == 'plot':
                if card.css('div._323d0f67'):
                    continue  # Skip this card if the div is present
            feature = {
                'price': card.css('span[aria-label="Price"]::text').get(),
                # 'location': card.css('div[aria-label="Location"]::text').get(),
                'details_url': 'https://www.zameen.com' + card.css('a::attr(href)').get(),
                'bedrooms': card.css('span[aria-label="Beds"]::text').get(),
                'bathrooms': card.css('span[aria-label="Baths"]::text').get(),
                'area': card.css('span[aria-label="Area"] *::text').get(),
                'int_price': '',
                'latitude': '',
                'longitude': '',
                'phone': 'N/A',
                'contact_name': 'N/A',
                'property_type': self.property
            }

            # Append the feature to the class-level features list
            self.features.append(feature)

        # Extract additional data after parsing the property cards
        try:
            # omit = False
            json_data = ''.join([
                script.get() for script in
                response.css('script::text')
                if 'window.state = ' in script.get()
            ])

            json_data = json_data.split(
                'window.state = ')[-1].split('}};')[0] + '}}'
            json_data = json.loads(json_data)

            # Extract the additional data
            additional_data = json_data['algolia']['content']['hits']

            for index, feat in enumerate(self.features):
                additional_info = additional_data[index]
                feat['int_price'] = additional_info['price']
                feat['latitude'] = additional_info['geography']['lat']
                feat['longitude'] = additional_info['geography']['lng']
                feat['phone'] = additional_info['phoneNumber']['mobileNumbers'][0]
                feat['contact_name'] = additional_info['contactName']

        except Exception as e:
            # Handle the exception if there's an issue with extracting additional data
            logging.error(f"Error extracting additional data: {str(e)}")

        # Yield each feature separately to be processed by the pipeline
        for home in self.features:
            if home['bedrooms'] is None:
                home['bedrooms'] = '-'
            if home['bathrooms'] is None:
                home['bathrooms'] = '-'
            if home['latitude'] != '' and home['longitude'] != '' and home['area'] is not None:
                homes += 1
                yield home


class IslamabadHouses(ZameenScraper):
    name = 'isb_house'
    property = 'house'
    base_url = 'https://www.zameen.com/Houses_Property/Islamabad-3-'


class RawalpindiHouses(ZameenScraper):
    name = 'rwp_house'
    property = 'house'
    base_url = 'https://www.zameen.com/Houses_Property/Rawalpindi-41-'


class IslamabadPlots(ZameenScraper):
    name = 'isb_plots'
    property = 'plot'
    base_url = 'https://www.zameen.com/Plots/Islamabad-3-'


class RawalpindiPlots(ZameenScraper):
    name = 'rwp_plots'
    property = 'plot'
    base_url = 'https://www.zameen.com/Plots/Rawalpindi-41-'


if __name__ == '__main__':
    # Set the logging level to WARNING
    logging.getLogger('scrapy').propagate = False

    # Capture the start time
    start_time = time.time()

    # Start the crawlers using spider classes
    process = CrawlerProcess(settings={
        "USER_AGENT": "...",
        "ITEM_PIPELINES": {'__main__.DataPipeline': 1},
        "LOG_LEVEL": logging.WARNING
    })

    process.crawl(IslamabadHouses)
    process.crawl(RawalpindiHouses)
    process.crawl(IslamabadPlots)
    process.crawl(RawalpindiPlots)
    print('Scrapper started')
    process.start()

    end_time = time.time()
    elapsed_time = end_time - start_time
    minutes = int(elapsed_time // 60)
    seconds = elapsed_time % 60
    print(total_pages)
    print(f"Spider runtime: {minutes:.0f} minutes {seconds:.2f} seconds")

    # Check if 'data.db' exists in 'files' directory
    if os.path.exists(os.path.join('files', 'data.db')):
        # Remove 'data.db' from 'files' directory
        os.remove(os.path.join('files', 'data.db'))
        print("Removed 'data.db' from 'files' directory")

    # Check if 'data.db' exists in the main directory
    if os.path.exists('data.db'):
        # Create the 'files' directory if it doesn't exist
        if not os.path.exists('files'):
            os.mkdir('files')

        # Move 'data.db' to 'files' directory
        shutil.move('data.db', os.path.join('files', 'data.db'))
        print("Moved 'data.db' to 'files' directory")
