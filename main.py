#!/usr/bin/env python3
"""
Amazon Wireless Headphones Scraper
Optimized for low memory usage on Apify Free Plan
"""

import asyncio
import json
import re
import time
import random
from typing import Dict, List, Optional
from urllib.parse import urljoin, urlparse, parse_qs
import requests
from bs4 import BeautifulSoup
from apify import Actor

class AmazonScraper:
    def __init__(self):
        self.session = requests.Session()
        self.setup_session()
        self.products = []
        self.processed_asins = set()
        
    def setup_session(self):
        """Configure session with headers and settings"""
        self.session.headers.update({
            'User-Agent': self.get_random_user_agent(),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0'
        })
        
    def get_random_user_agent(self) -> str:
        """Return random user agent to avoid detection"""
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15'
        ]
        return random.choice(user_agents)
    
    def make_request(self, url: str, retries: int = 3) -> Optional[BeautifulSoup]:
        """Make HTTP request with retry logic"""
        for attempt in range(retries):
            try:
                # Random delay to avoid rate limiting
                time.sleep(random.uniform(1, 3))
                
                response = self.session.get(url, timeout=30)
                
                if response.status_code == 200:
                    # Parse with lxml for speed, fallback to html.parser
                    try:
                        return BeautifulSoup(response.content, 'lxml')
                    except:
                        return BeautifulSoup(response.content, 'html.parser')
                        
                elif response.status_code == 503:
                    Actor.log.warning(f"Service unavailable (503), retrying... Attempt {attempt + 1}")
                    time.sleep(random.uniform(5, 10))
                    continue
                    
                else:
                    Actor.log.warning(f"Status code {response.status_code} for {url}")
                    
            except Exception as e:
                Actor.log.error(f"Request failed (attempt {attempt + 1}): {str(e)}")
                if attempt < retries - 1:
                    time.sleep(random.uniform(2, 5))
                    
        return None
    
    def extract_product_links(self, soup: BeautifulSoup) -> List[str]:
        """Extract product links from search results page"""
        links = []
        
        # Multiple selectors for different Amazon layouts
        selectors = [
            '[data-component-type="s-search-result"] h2 a',
            '[data-component-type="s-search-result"] .a-link-normal',
            '.s-result-item h2 a',
            '.s-result-item .a-link-normal'
        ]
        
        for selector in selectors:
            elements = soup.select(selector)
            for element in elements:
                href = element.get('href')
                if href and '/dp/' in href:
                    # Convert relative URL to absolute
                    full_url = urljoin('https://www.amazon.com', href)
                    # Clean URL (remove query parameters)
                    clean_url = full_url.split('?')[0]
                    if clean_url not in links:
                        links.append(clean_url)
        
        return links
    
    def extract_asin(self, url: str) -> Optional[str]:
        """Extract ASIN from product URL"""
        match = re.search(r'/dp/([A-Z0-9]{10})', url)
        return match.group(1) if match else None
    
    def extract_product_data(self, soup: BeautifulSoup, url: str) -> Optional[Dict]:
        """Extract product information from product page"""
        try:
            product = {
                'url': url,
                'asin': self.extract_asin(url),
                'scraped_at': time.strftime('%Y-%m-%d %H:%M:%S')
            }
            
            # Title - multiple selectors
            title_selectors = ['#productTitle', '[data-feature-name="title"] h1', '.product-title']
            for selector in title_selectors:
                element = soup.select_one(selector)
                if element:
                    product['title'] = element.get_text().strip()
                    break
            else:
                product['title'] = None
            
            # Price - multiple selectors
            price_selectors = [
                '.a-price-whole',
                '.a-price .a-offscreen',
                '.a-price-range .a-price .a-offscreen',
                '#price_inside_buybox',
                '.a-color-price'
            ]
            for selector in price_selectors:
                element = soup.select_one(selector)
                if element:
                    price_text = element.get_text().strip()
                    # Extract numeric price
                    price_match = re.search(r'[\d,]+\.?\d*', price_text.replace(',', ''))
                    if price_match:
                        try:
                            product['price'] = float(price_match.group().replace(',', ''))
                        except:
                            product['price'] = price_text
                        break
            else:
                product['price'] = None
            
            # Rating
            rating_selectors = [
                '[data-hook="average-star-rating"] .a-icon-alt',
                '#acrPopover .a-icon-alt',
                '.a-icon-star .a-icon-alt'
            ]
            for selector in rating_selectors:
                element = soup.select_one(selector)
                if element:
                    rating_text = element.get_text().strip()
                    rating_match = re.search(r'(\d+\.?\d*)', rating_text)
                    if rating_match:
                        try:
                            product['rating'] = float(rating_match.group(1))
                        except:
                            product['rating'] = rating_text
                        break
            else:
                product['rating'] = None
            
            # Review count
            review_selectors = [
                '[data-hook="total-review-count"]',
                '#acrCustomerReviewText',
                '.a-size-base'
            ]
            for selector in review_selectors:
                element = soup.select_one(selector)
                if element:
                    review_text = element.get_text().strip()
                    review_match = re.search(r'([\d,]+)', review_text.replace(',', ''))
                    if review_match:
                        try:
                            product['review_count'] = int(review_match.group(1).replace(',', ''))
                        except:
                            product['review_count'] = review_text
                        break
            else:
                product['review_count'] = None
            
            # Brand
            brand_selectors = [
                '#bylineInfo',
                '[data-feature-name="bylineInfo"] a',
                '.a-text-bold'
            ]
            for selector in brand_selectors:
                element = soup.select_one(selector)
                if element:
                    brand_text = element.get_text().strip()
                    # Clean brand text
                    brand_text = re.sub(r'^(Brand:|Visit the|by)\s*', '', brand_text, flags=re.IGNORECASE)
                    brand_text = re.sub(r'\s*Store$', '', brand_text, flags=re.IGNORECASE)
                    if brand_text:
                        product['brand'] = brand_text
                        break
            else:
                product['brand'] = None
            
            # Availability
            availability_selectors = [
                '#availability span',
                '#availability .a-color-success',
                '#availability .a-color-state'
            ]
            for selector in availability_selectors:
                element = soup.select_one(selector)
                if element:
                    product['availability'] = element.get_text().strip()
                    break
            else:
                product['availability'] = None
            
            # Image URL
            image_selectors = ['#landingImage', '.a-dynamic-image', '#main-image']
            for selector in image_selectors:
                element = soup.select_one(selector)
                if element:
                    product['image_url'] = element.get('src') or element.get('data-src')
                    break
            else:
                product['image_url'] = None
            
            # Price filter - only include if under $100
            if product['price'] and isinstance(product['price'], (int, float)):
                if product['price'] > 100:
                    return None
            
            return product
            
        except Exception as e:
            Actor.log.error(f"Error extracting product data: {str(e)}")
            return None
    
    def get_next_page_url(self, soup: BeautifulSoup, current_url: str) -> Optional[str]:
        """Get next page URL from search results"""
        next_selectors = [
            'a[aria-label="Go to next page"]',
            '.s-pagination-next',
            '.pagnNextLink'
        ]
        
        for selector in next_selectors:
            element = soup.select_one(selector)
            if element and element.get('href'):
                return urljoin('https://www.amazon.com', element.get('href'))
        
        return None
    
    async def scrape_search_results(self, start_url: str, max_products: int = 20000):
        """Scrape search results and product pages"""
        current_url = start_url
        page_count = 0
        
        while current_url and len(self.products) < max_products:
            page_count += 1
            Actor.log.info(f"Scraping page {page_count}: {current_url}")
            
            # Get search results page
            soup = self.make_request(current_url)
            if not soup:
                Actor.log.error(f"Failed to load search page: {current_url}")
                break
            
            # Extract product links
            product_links = self.extract_product_links(soup)
            Actor.log.info(f"Found {len(product_links)} product links on page {page_count}")
            
            # Process each product
            for link in product_links:
                if len(self.products) >= max_products:
                    break
                    
                asin = self.extract_asin(link)
                if asin and asin in self.processed_asins:
                    continue
                    
                Actor.log.info(f"Scraping product: {link}")
                
                # Get product page
                product_soup = self.make_request(link)
                if not product_soup:
                    continue
                
                # Extract product data
                product_data = self.extract_product_data(product_soup, link)
                if product_data:
                    self.products.append(product_data)
                    if asin:
                        self.processed_asins.add(asin)
                    
                    # Push data to Apify dataset
                    await Actor.push_data(product_data)
                    
                    Actor.log.info(f"Scraped product: {product_data.get('title', 'Unknown')} - ${product_data.get('price', 'N/A')}")
                
                # Memory management - clear variables
                del product_soup, product_data
            
            # Get next page URL
            current_url = self.get_next_page_url(soup, current_url)
            
            # Clear memory
            del soup, product_links
            
            Actor.log.info(f"Total products scraped: {len(self.products)}")
            
            # Respect rate limits
            time.sleep(random.uniform(2, 4))

async def main():
    async with Actor:
        # Get input
        input_data = await Actor.get_input() or {}
        
        start_url = input_data.get('startUrl', 
            'https://www.amazon.com/s?k=wireless+headphones&rh=p_36%3A-10000&ref=sr_nr_p_36_1')
        max_products = input_data.get('maxProducts', 20000)
        
        Actor.log.info(f"Starting Amazon scraper with URL: {start_url}")
        Actor.log.info(f"Max products to scrape: {max_products}")
        
        # Initialize scraper
        scraper = AmazonScraper()
        
        # Start scraping
        await scraper.scrape_search_results(start_url, max_products)
        
        Actor.log.info(f"Scraping completed! Total products: {len(scraper.products)}")

if __name__ == '__main__':
    asyncio.run(main())