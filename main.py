#!/usr/bin/env python3
"""
Amazon Wireless Headphones Scraper with Smart Session Rotation
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
    def __init__(self, min_delay=1.0, max_delay=3.0, request_timeout=30, show_ip=False, 
                 proxy_configuration=None, session_rotation_enabled=False, 
                 session_min_requests=30, session_max_requests=50):
        self.session = requests.Session()
        self.setup_session()
        self.products = []
        self.processed_asins = set()
        
        # Configurable parameters
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.request_timeout = request_timeout
        self.show_ip = show_ip
        self.proxy_configuration = proxy_configuration
        self.proxy_stats = {'total_requests': 0, 'unique_ips': set()}
        
        # Session rotation parameters
        self.session_rotation_enabled = session_rotation_enabled
        self.session_min_requests = session_min_requests
        self.session_max_requests = session_max_requests
        self.current_session_requests = 0
        self.session_rotation_limit = random.randint(session_min_requests, session_max_requests) if session_rotation_enabled else float('inf')
        self.current_session_id = f"session_{int(time.time())}"
        self.session_count = 0
        self.emergency_rotations = 0
        
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
    
    async def rotate_session_if_needed(self):
        """Rotate session if the current session has reached its limit"""
        if not self.session_rotation_enabled:
            return False
            
        if self.current_session_requests >= self.session_rotation_limit:
            # Time to rotate!
            self.session_count += 1
            old_session_id = self.current_session_id
            self.current_session_id = f"session_{int(time.time())}_{self.session_count}"
            self.current_session_requests = 0
            self.session_rotation_limit = random.randint(self.session_min_requests, self.session_max_requests)
            
            Actor.log.info(f"🔄 SESSION ROTATION #{self.session_count}")
            Actor.log.info(f"   📤 Old session: {old_session_id[-20:]}")
            Actor.log.info(f"   📥 New session: {self.current_session_id[-20:]}")
            Actor.log.info(f"   🎯 Next rotation in: {self.session_rotation_limit} requests")
            Actor.log.info(f"   🌐 Expecting new IP on next request...")
            
            return True
        return False

    async def check_current_ip(self, proxies=None):
        """Check and return current IP address"""
        try:
            ip_response = self.session.get('https://httpbin.org/ip', timeout=5, proxies=proxies)
            if ip_response.status_code == 200:
                ip_data = ip_response.json()
                return ip_data.get('origin', 'Unknown')
        except Exception as e:
            Actor.log.warning(f"Failed to check IP: {str(e)}")
        return None
    
    async def make_request(self, url: str, retries: int = 3) -> Optional[BeautifulSoup]:
        """Make HTTP request with retry logic and smart session rotation"""
        
        # Check if we need to rotate session
        session_rotated = await self.rotate_session_if_needed()
        
        for attempt in range(retries):
            try:
                # Use configurable delay
                time.sleep(random.uniform(self.min_delay, self.max_delay))
                
                # Get proxy URL if configuration exists
                proxy_url = None
                if self.proxy_configuration:
                    # Use current session ID for consistent IP within session
                    proxy_url = await self.proxy_configuration.new_url(session_id=self.current_session_id)
                
                # Setup proxies for requests
                proxies = None
                if proxy_url:
                    proxies = {
                        'http': proxy_url,
                        'https': proxy_url
                    }
                
                # Show IP if enabled
                if self.show_ip and attempt == 0:  # Only show on first attempt
                    current_ip = await self.check_current_ip(proxies)
                    if current_ip:
                        self.proxy_stats['unique_ips'].add(current_ip)
                        rotation_status = "🔄 ROTATED" if session_rotated else "📍 SAME"
                        session_progress = f"({self.current_session_requests + 1}/{self.session_rotation_limit})"
                        Actor.log.info(f"🌐 {rotation_status} IP: {current_ip} {session_progress} for {url[:50]}...")
                
                self.proxy_stats['total_requests'] += 1
                self.current_session_requests += 1
                
                # Make the request with or without proxy
                response = self.session.get(url, timeout=self.request_timeout, proxies=proxies)
                
                if response.status_code == 200:
                    # Parse with lxml for speed, fallback to html.parser
                    try:
                        return BeautifulSoup(response.content, 'lxml')
                    except:
                        return BeautifulSoup(response.content, 'html.parser')
                        
                elif response.status_code == 503:
                    Actor.log.warning(f"Service unavailable (503) - Amazon blocking! Force rotating session...")
                    
                    # Force immediate session rotation on 503
                    if self.proxy_configuration and self.session_rotation_enabled:
                        self.emergency_rotations += 1
                        self.session_count += 1
                        old_session_id = self.current_session_id
                        self.current_session_id = f"emergency_session_{int(time.time())}_{self.session_count}"
                        self.current_session_requests = 0
                        self.session_rotation_limit = random.randint(self.session_min_requests, self.session_max_requests)
                        
                        Actor.log.info(f"🚨 EMERGENCY SESSION ROTATION #{self.session_count} (Emergency #{self.emergency_rotations})")
                        Actor.log.info(f"   🔄 Forced rotation due to 503 error")
                        Actor.log.info(f"   📥 New emergency session: {self.current_session_id[-25:]}")
                        Actor.log.info(f"   🎯 Next rotation in: {self.session_rotation_limit} requests")
                    elif self.proxy_configuration:
                        # Even without session rotation, try to get new proxy
                        Actor.log.info(f"🚨 503 ERROR - Attempting to get fresh proxy...")
                    else:
                        Actor.log.info(f"🚨 503 ERROR - No proxy available, cooling down...")
                    
                    # Longer delay after 503 to cool down
                    time.sleep(random.uniform(10, 15))
                    continue
                    
                else:
                    Actor.log.warning(f"Status code {response.status_code} for {url}")
                    
            except Exception as e:
                Actor.log.error(f"Request failed (attempt {attempt + 1}): {str(e)}")
                if attempt < retries - 1:
                    time.sleep(random.uniform(self.min_delay * 2, self.max_delay * 2))
                    
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
            soup = await self.make_request(current_url)
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
                product_soup = await self.make_request(link)
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
            
            # Log proxy stats if enabled
            if self.show_ip:
                session_info = f"Session #{self.session_count + 1}" if self.session_rotation_enabled else "No session rotation"
                Actor.log.info(f"📊 Proxy Stats - Total: {self.proxy_stats['total_requests']}, Unique IPs: {len(self.proxy_stats['unique_ips'])}, {session_info}")
            
            # Respect rate limits
            time.sleep(random.uniform(self.min_delay * 1.5, self.max_delay * 1.5))

async def main():
    async with Actor:
        # Get input
        input_data = await Actor.get_input() or {}
        
        start_url = input_data.get('startUrl', 
            'https://www.amazon.com/s?k=wireless+headphones&rh=p_36%3A-10000&ref=sr_nr_p_36_1')
        max_products = input_data.get('maxProducts', 20000)
        
        # Performance & Debug Parameters
        min_delay = input_data.get('minDelay', 1.0)
        max_delay = input_data.get('maxDelay', 3.0)
        request_timeout = input_data.get('requestTimeout', 30)
        show_ip = input_data.get('showIP', False)
        
        # Proxy Parameters
        use_proxy = input_data.get('useProxy', False)
        proxy_groups = input_data.get('proxyGroups', ['RESIDENTIAL'])
        proxy_country = input_data.get('proxyCountry', None)
        
        # Session rotation parameters  
        session_rotation_enabled = input_data.get('sessionRotationEnabled', False)
        session_min_requests = input_data.get('sessionMinRequests', 30)
        session_max_requests = input_data.get('sessionMaxRequests', 50)
        
        Actor.log.info(f"🚀 Starting Amazon scraper with URL: {start_url}")
        Actor.log.info(f"📊 Max products: {max_products}")
        Actor.log.info(f"⏱️  Delay range: {min_delay}s - {max_delay}s")
        Actor.log.info(f"⌛ Request timeout: {request_timeout}s")
        Actor.log.info(f"🌐 Show IP: {show_ip}")
        Actor.log.info(f"🔄 Use Proxy: {use_proxy}")
        if use_proxy:
            Actor.log.info(f"📍 Proxy Groups: {proxy_groups}")
            if proxy_country:
                Actor.log.info(f"🌍 Proxy Country: {proxy_country}")
        
        # Session rotation info
        if session_rotation_enabled:
            Actor.log.info(f"🎯 Session Rotation: ENABLED")
            Actor.log.info(f"   📊 Requests per session: {session_min_requests}-{session_max_requests}")
        else:
            Actor.log.info(f"🎯 Session Rotation: DISABLED (single session)")
        
        # Setup proxy configuration
        proxy_configuration = None
        if use_proxy:
            try:
                proxy_config_params = {'groups': proxy_groups}
                if proxy_country:
                    proxy_config_params['country_code'] = proxy_country
                    
                proxy_configuration = await Actor.create_proxy_configuration(**proxy_config_params)
                
                if proxy_configuration:
                    Actor.log.info(f"✅ Proxy configuration created successfully!")
                else:
                    Actor.log.warning(f"⚠️ Failed to create proxy configuration, continuing without proxy")
                    
            except Exception as e:
                Actor.log.error(f"❌ Proxy setup failed: {str(e)}")
                Actor.log.info(f"🔄 Continuing without proxy...")
        
        # Initialize scraper with parameters
        scraper = AmazonScraper(
            min_delay=min_delay,
            max_delay=max_delay, 
            request_timeout=request_timeout,
            show_ip=show_ip,
            proxy_configuration=proxy_configuration,
            session_rotation_enabled=session_rotation_enabled,
            session_min_requests=session_min_requests,
            session_max_requests=session_max_requests
        )
        
        # Start scraping
        await scraper.scrape_search_results(start_url, max_products)
        
        Actor.log.info(f"✅ Scraping completed! Total products: {len(scraper.products)}")
        Actor.log.info(f"📈 Average delay used: {(min_delay + max_delay) / 2:.1f}s")
        
        # Final proxy stats
        if show_ip and scraper.proxy_stats['total_requests'] > 0:
            unique_ip_count = len(scraper.proxy_stats['unique_ips'])
            Actor.log.info(f"🎯 Final Proxy Stats:")
            Actor.log.info(f"   📡 Total requests: {scraper.proxy_stats['total_requests']}")
            Actor.log.info(f"   🌐 Unique IPs used: {unique_ip_count}")
            Actor.log.info(f"   🔄 IP rotation rate: {unique_ip_count/scraper.proxy_stats['total_requests']*100:.1f}%")
            
            if session_rotation_enabled:
                Actor.log.info(f"   🎯 Total sessions created: {scraper.session_count + 1}")
                if scraper.emergency_rotations > 0:
                    Actor.log.info(f"   🚨 Emergency rotations (503 errors): {scraper.emergency_rotations}")
                avg_requests_per_session = scraper.proxy_stats['total_requests'] / max(1, scraper.session_count + 1)
                Actor.log.info(f"   📊 Avg requests per session: {avg_requests_per_session:.1f}")
                
            if unique_ip_count > 1:
                Actor.log.info(f"   ✅ IP rotation working perfectly!")
            elif session_rotation_enabled and scraper.session_count > 0:
                Actor.log.info(f"   🔄 Sessions rotated but same IP pool (normal for some proxies)")
            else:
                Actor.log.info(f"   ⚠️ No IP rotation detected")

if __name__ == '__main__':
    asyncio.run(main())