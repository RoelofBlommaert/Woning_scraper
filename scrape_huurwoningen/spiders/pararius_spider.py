import scrapy
from bs4 import BeautifulSoup
import re

class ParariusSpider(scrapy.Spider):
    name = 'pararius'
    start_urls = [
        'https://www.pararius.com/apartments/rotterdam',  # Base URL
    ]

    def parse(self, response):

        soup = BeautifulSoup(response.body, 'html.parser')

        # Find all listing links
        links = soup.find_all('a', href=True, class_='listing-search-item__link listing-search-item__link--title')

        if not links:
            print("No rental links found on Pararius")

        for link in links:
            url = link['href']
            if url.startswith('/'):
                url = f'https://www.pararius.com{url}'  # Make the URL absolute

            print(f"Found URL: {url}")
            # Crawl the detail page of each listing
            yield response.follow(url, callback=self.parse_listing)

        # Check for next page link and follow pagination
        current_url = response.url
        if '/page-' in current_url:
            current_page = int(current_url.split('/page-')[-1])
        else:
            current_page = 1  # For the first page, current_page is 1
        
        next_page = current_page + 1
        next_page_url = f'https://www.pararius.com/apartments/rotterdam/page-{next_page}'

        yield response.follow(next_page_url, callback=self.parse)

    def parse_listing(self, response):
        soup = BeautifulSoup(response.body, 'html.parser')

        # Extract the details from the listing page
        title = soup.find('h1', class_='listing-detail-summary__title').text.strip() if soup.find('h1', class_='listing-detail-summary__title') else None
        price = soup.find('span', class_='listing-detail-summary__price-main').text.strip() if soup.find('span', class_='listing-detail-summary__price-main') else None
        location = soup.find('div', class_='listing-detail-summary__location').text.strip() if soup.find('div', class_='listing-detail-summary__location') else None
        square_meters = soup.find('li', class_='illustrated-features__item--surface-area').text.strip() if soup.find('li', class_='illustrated-features__item--surface-area') else None
        rooms = soup.find('li', class_='illustrated-features__item--number-of-rooms').text.strip() if soup.find('li', class_='illustrated-features__item--number-of-rooms') else None
        interior_state = soup.find('li', class_='illustrated-features__item--interior').text.strip() if soup.find('li', class_='illustrated-features__item--interior') else None

        # Clean data
        price_cleaned = int(re.sub(r'[^\d]', '', price)) if price else None
        square_meters_cleaned = int(re.sub(r'[^\d]', '', square_meters)) if square_meters else None
        rooms_cleaned = int(re.search(r'\d+', rooms).group()) if rooms else None
        postal_code = re.search(r'(\d{4} [A-Z]{2})', location).group() if location else None
        neighborhood = re.search(r'\((.*?)\)', location).group(1) if location and '(' in location else None

        # Yield cleaned data
        yield {
            'title': title,
            'price': price_cleaned,
            'square_meters': square_meters_cleaned,
            'rooms': rooms_cleaned,
            'interior_state': interior_state,
            'postal_code': postal_code,
            'neighborhood': neighborhood,
            'url': response.url,  # URL of the listing
        }
