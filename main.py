import json
import time
import asyncio
import aiohttp
from bs4 import BeautifulSoup as beauty


class Parser:
    def __init__(self, url, output_file):
        # initilization
        self.url = url + "?in_stock=1" # in_stock=1 prods in stock
        self.base_url = '/'.join(url.split('/')[:3]) # base url for forming abolute links
        self.output_file = output_file
        self.products_data = []

    # load an html page form a given url
    async def fetch_page(self, url):
        headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:88.0) Gecko/20100101 Firefox/88.0'}

        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, headers=headers) as response:
                    if response.status == 200:
                        return await response.text()
                    else:
                        print(f"failed to fetch a page: {url} (Status {response.status})")
                        return None
            except aiohttp.ClientError as e:
                print(f"failed to fetch a page: {url} ({e})")
                return None

    # parsing product info
    async def scrape_product(self, product_card):
        product_link = product_card.find('a', class_='product-card-name')['href']
        brand_url = self.base_url + product_link

        # load a page to get prod brand info
        brand_response = await self.fetch_page(brand_url)
        if not brand_response:
            print(f"Failed to fetch brand data for {brand_url}")
            return None

        soup = beauty(brand_response, 'html.parser')
        brand = soup.find('meta', itemprop='brand')
        brand = brand['content'] if brand else None

        # extracting prod data
        data_sku = product_card.get('data-sku')
        product_title_e = product_card.find('span', class_='product-card-name__text')
        product_title = product_title_e.text.strip() if product_title_e else "no title found"

        actual_price_wrapper = product_card.find('div', class_='product-unit-prices__actual-wrapper')
        actual_price = ""

        if actual_price_wrapper:
            actual_price_rub_e = actual_price_wrapper.find('span', class_='product-price__sum-rubles')
            actual_price_pe_e = actual_price_wrapper.find('span', class_='product-price__sum-penny')
            actual_price_rub = actual_price_rub_e.text.strip() if actual_price_rub_e else ""
            actual_price_pe = actual_price_pe_e.text.strip() if actual_price_pe_e else ""
            actual_price = f"{actual_price_rub}{actual_price_pe}"

        old_price_wrapper = product_card.find('div', class_='product-unit-prices__old-wrapper')
        old_price = ""
        if old_price_wrapper:
            old_price_rub_e = old_price_wrapper.find('span', class_='product-price__sum-rubles')
            if old_price_rub_e:
                old_price_rub = old_price_rub_e.text.strip()
                old_price_pe_e = old_price_wrapper.find('span', class_='product-price__sum-penny')
                old_price_pe = old_price_pe_e.text.strip() if old_price_pe_e else ""
                old_price = f"{old_price_rub}{old_price_pe}"
            else:
                old_price = actual_price
                actual_price = ""

        product_data = {
            "id": data_sku,
            "name": product_title,
            "link": self.base_url + product_link,
            "regular price": old_price,
            "promo price": actual_price,
            "brand": brand
        }
        self.products_data.append(product_data)

    # parsing all prods on a category page
    async def scrape_products(self, url):
        response = await self.fetch_page(url)
        if not response:
            print(f"failed to fetch products: {url}")
            return

        soup = beauty(response, 'html.parser')
        products_inner = soup.find('div', {'id': 'products-inner'})
        if not products_inner:
            print(f"no product found: {url}")
            return

        product_cards = products_inner.find_all('div', {'data-sku': True})
        tasks = [self.scrape_product(card) for card in product_cards]
        await asyncio.gather(*tasks)

    # parsing all category pages
    async def scrape_all_pages(self):
        response = await self.fetch_page(self.url)

        if not response:
            print(f"failed to fetch initial page: {self.url}")
            return

        soup = beauty(response, 'html.parser')
        pagination_items = soup.find_all('a', class_='v-pagination__item')
        if not pagination_items:
            print("pagination not found")
            return

        max_page = int(pagination_items[-1].text.strip())
        tasks = [self.scrape_products(f"{self.url}&page={page}") for page in range(1, max_page + 1)]
        await asyncio.gather(*tasks)

        # save in json
        with open(self.output_file, 'w', encoding='utf-8') as f:
            json.dump(self.products_data, f, ensure_ascii=False, indent=4)


async def main():
    url = "https://online.metro-cc.ru/category/bezalkogolnye-napitki/pityevaya-voda-kulery" 

    start_time = time.time()

    scraper = Parser(url, "data.json")
    await scraper.scrape_all_pages()

    end_time = time.time()
    elapsed_time = end_time - start_time

    total_products = len(scraper.products_data)
    print(f"count: {total_products}")
    print(f"time: {elapsed_time:.2f} sec")
    

if __name__ == "__main__":
    asyncio.run(main())
