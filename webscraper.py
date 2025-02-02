"""
Advanced Web Scraper with:
- Concurrent scraping
- Data validation
- Multiple output formats
- Error handling
- Proxy rotation
- User agent rotation
- Caching
- CLI interface
- Color output
"""

import os
import re
import csv
import json
import time
import random
import sqlite3
import argparse
import logging
from typing import List, Dict, Optional
from datetime import datetime
from dataclasses import dataclass, asdict
import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
from tqdm import tqdm
import colorama
from colorama import Fore, Style

# Initialize colorama
colorama.init(autoreset=True)

# Configuration
DEFAULT_CONFIG = {
    "max_depth": 2,
    "max_workers": 5,
    "timeout": 10,
    "cache_ttl": 3600,
    "output_formats": ["csv", "json"],
    "allowed_domains": [],
    "proxies": [],
    "user_agents": []
}

# Data class for scraped items
@dataclass
class ScrapedItem:
    url: str
    title: str
    content: str
    timestamp: datetime
    links: List[str]
    status_code: int
    metadata: dict

class WebScraper:
    def __init__(self, config: dict = None):
        self.config = {**DEFAULT_CONFIG, **(config or {})}
        self.visited_urls = set()
        self.session = requests.Session()
        self.logger = self._setup_logger()
        self._setup_user_agents()
        self._setup_proxies()
        self.cache = {}
        # Initialize database
        self.conn = sqlite3.connect('scraper.db', check_same_thread=False)
        self.conn.execute('PRAGMA journal_mode=WAL')
        self._init_db()

    def _setup_logger(self) -> logging.Logger:
        logger = logging.getLogger('WebScraper')
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter(
            f'{Fore.CYAN}%(asctime)s {Fore.YELLOW}%(levelname)s:{Style.RESET_ALL} %(message)s'
        )
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        return logger

    def _setup_user_agents(self):
        if not self.config["user_agents"]:
            self.user_agents = [UserAgent().random for _ in range(5)]
        else:
            self.user_agents = self.config["user_agents"]

    def _setup_proxies(self):
        if not self.config["proxies"]:
            self.proxies = []
        else:
            self.proxies = self.config["proxies"]

    def _init_db(self):
        cursor = self.conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS scraped_data (
                id INTEGER PRIMARY KEY,
                url TEXT UNIQUE,
                title TEXT,
                content TEXT,
                timestamp DATETIME,
                status_code INTEGER,
                metadata TEXT
            )
        ''')
        self.conn.commit()

    def _get_random_headers(self) -> dict:
        return {
            'User-Agent': random.choice(self.user_agents),
            'Accept-Language': 'en-US,en;q=0.5',
            'Referer': 'https://www.google.com/'
        }

    def _is_valid_url(self, url: str) -> bool:
        parsed = urlparse(url)
        if self.config["allowed_domains"]:
            return parsed.netloc in self.config["allowed_domains"]
        return True

    def _should_cache(self, url: str) -> bool:
        if url in self.cache:
            cached_time = self.cache[url]['timestamp']
            return (datetime.now() - cached_time).seconds < self.config["cache_ttl"]
        return False

    def fetch_page(self, url: str) -> Optional[requests.Response]:
        if self._should_cache(url):
            self.logger.info(f"{Fore.GREEN}Using cached: {url}")
            return self.cache[url]['response']
        try:
            proxies = {'http': random.choice(self.proxies)} if self.proxies else None
            response = self.session.get(
                url,
                headers=self._get_random_headers(),
                timeout=self.config["timeout"],
                proxies=proxies,
                allow_redirects=True
            )
            response.raise_for_status()
            # Cache the response
            self.cache[url] = {
                'response': response,
                'timestamp': datetime.now()
            }
            return response
        except requests.exceptions.RequestException as e:
            self.logger.error(f"{Fore.RED}Error fetching {url}: {str(e)}")
            return None

    def parse_page(self, response: requests.Response) -> ScrapedItem:
        soup = BeautifulSoup(response.content, 'html.parser')
        for element in soup(['script', 'style', 'nav', 'footer']):
            element.decompose()
        
        title = soup.title.string.strip() if soup.title else ''
        content = ' '.join(soup.stripped_strings)
        links = [urljoin(response.url, link.get('href')) for link in soup.find_all('a')]
        
        metadata = {
            'language': soup.find('html').get('lang', ''),
            'description': soup.find('meta', attrs={'name': 'description'}).get('content', '') if soup.find('meta', attrs={'name': 'description'}) else '',
            'keywords': soup.find('meta', attrs={'name': 'keywords'}).get('content', '') if soup.find('meta', attrs={'name': 'keywords'}) else '',
            'canonical': soup.find('link', rel='canonical').get('href', '') if soup.find('link', rel='canonical') else ''
        }
        
        return ScrapedItem(
            url=response.url,
            title=title,
            content=content,
            timestamp=datetime.now(),
            links=links,
            status_code=response.status_code,
            metadata=metadata
        )

    def scrape(self, start_urls: List[str]):
        with ThreadPoolExecutor(max_workers=self.config["max_workers"]) as executor:
            futures = {executor.submit(self._scrape_page, url, 0): url for url in start_urls}
            with tqdm(total=len(futures), desc=f"{Fore.BLUE}Scraping Pages{Style.RESET_ALL}") as pbar:
                for future in as_completed(futures):
                    pbar.update(1)
                    try:
                        future.result()
                    except Exception as e:
                        self.logger.error(f"{Fore.RED}Error in future: {str(e)}")

    def _scrape_page(self, url: str, depth: int):
        if depth > self.config["max_depth"] or url in self.visited_urls:
            return
        
        self.visited_urls.add(url)
        if not self._is_valid_url(url):
            self.logger.warning(f"{Fore.YELLOW}Skipping invalid URL: {url}")
            return
        
        response = self.fetch_page(url)
        if not response:
            return
        
        item = self.parse_page(response)
        self._store_data(item)
        
        if depth < self.config["max_depth"]:
            for link in item.links:
                if link not in self.visited_urls:
                    self._scrape_page(link, depth + 1)

    def _store_data(self, item: ScrapedItem):
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT OR IGNORE INTO scraped_data
            (url, title, content, timestamp, status_code, metadata)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            item.url,
            item.title,
            item.content,
            item.timestamp.isoformat(),
            item.status_code,
            json.dumps(item.metadata)
        ))
        self.conn.commit()

        data_dict = asdict(item)
        data_dict['timestamp'] = data_dict['timestamp'].isoformat()
        
        for fmt in self.config["output_formats"]:
            filename = f"output.{fmt}"
            if fmt == 'csv':
                with open(filename, 'a', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=data_dict.keys())
                    if f.tell() == 0:
                        writer.writeheader()
                    writer.writerow(data_dict)
            elif fmt == 'json':
                with open(filename, 'a', encoding='utf-8') as f:
                    json.dump(data_dict, f)
                    f.write('\n')

    def export_report(self):
        df = pd.read_sql_query("SELECT * FROM scraped_data", self.conn)
        report = f"""
        {Fore.CYAN}Scraping Report:
        {Fore.YELLOW}================
        {Fore.GREEN}Total Pages: {len(df)}
        {Fore.BLUE}Success Rate: {(df['status_code'] == 200).mean():.2%}
        {Fore.MAGENTA}Avg Content Length: {df['content'].str.len().mean():.0f} chars
        {Fore.WHITE}Unique Domains: {df['url'].apply(lambda x: urlparse(x).netloc).nunique()}
        """
        print(report)
        df.to_csv('final_report.csv', index=False)
        self.logger.info(f"{Fore.GREEN}Report generated: final_report.csv")

    def close(self):
        self.session.close()
        self.conn.close()

def main():
    parser = argparse.ArgumentParser(
        description=f"{Fore.GREEN}Advanced Web Scraper{Style.RESET_ALL}",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    parser.add_argument('urls', nargs='+', help="Starting URLs")
    parser.add_argument('--depth', type=int, default=2, help="Max crawling depth")
    parser.add_argument('--workers', type=int, default=5, help="Number of concurrent workers")
    parser.add_argument('--output', nargs='+', default=['csv', 'json'],
                      choices=['csv', 'json', 'db'], help="Output formats")
    parser.add_argument('--proxy-file', help="File containing proxy list")
    
    args = parser.parse_args()
    
    config = {
        "max_depth": args.depth,
        "max_workers": args.workers,
        "output_formats": args.output
    }
    
    if args.proxy_file:
        with open(args.proxy_file) as f:
            config["proxies"] = [line.strip() for line in f]
    
    scraper = WebScraper(config)
    try:
        scraper.scrape(args.urls)
        scraper.export_report()
    except KeyboardInterrupt:
        scraper.logger.info(f"{Fore.YELLOW}Scraping interrupted by user")
    finally:
        scraper.close()

if __name__ == '__main__':
    main()
