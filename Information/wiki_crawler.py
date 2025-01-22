import json
import requests
import re
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
import logging as logger
import os
import time
from collections import deque
import random
from threading import Thread

logger.basicConfig(
    level=logger.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class Wiki_Crawler:

    def __init__(self, start_url, directory, max_pages=10000):
        self.start_url = start_url
        self.directory = directory
        self.max_pages = max_pages
        self.visited_urls = set()
        self.url_queue = deque([start_url])
        self.pages_processed = 0
        self.wiki_page_link_pattern = re.compile(r"^/wiki/[^:#]*$")

        # Create directory if it doesn't exist
        if not os.path.exists(directory):
            os.makedirs(directory)

        # Load previously visited URLs if exists
        self.load_progress()

    def load_progress(self):
        """Load previously visited URLs to resume crawling."""
        progress_file = os.path.join(self.directory, "crawl_progress.json")
        if os.path.exists(progress_file):
            try:
                with open(progress_file, "r") as f:
                    data = json.load(f)
                    self.visited_urls = set(data.get("visited_urls", []))
                    self.pages_processed = len(self.visited_urls)
                    logger.info(
                        f"Loaded progress: {self.pages_processed} pages previously crawled"
                    )
            except Exception as e:
                logger.error(f"Error loading progress: {e}")

    def save_progress(self):
        """Save crawling progress periodically."""
        progress_file = os.path.join(self.directory, "crawl_progress.json")
        try:
            with open(progress_file, "w") as f:
                json.dump(
                    {
                        "visited_urls": list(self.visited_urls),
                        "pages_processed": self.pages_processed,
                    },
                    f,
                )
            logger.info(f"Progress saved: {self.pages_processed} pages")
        except Exception as e:
            logger.error(f"Error saving progress: {e}")

    def download_page(self, url):
        """Download page content with rate limiting."""
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }

        # Request Delay
        time.sleep(random.uniform(1, 3))

        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response.text
        except Exception as e:
            logger.error(f"Error downloading {url}: {e}")
            return None

    def store_page(self, data):
        """Store page content in JSON file."""
        if not data.get("title"):
            return

        file_name = re.sub(r'[<>:"/\\|?*]', "_", data["title"])
        file_path = os.path.join(self.directory, f"{file_name}.json")

        try:
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4, ensure_ascii=False)
            self.pages_processed += 1
            logger.info(
                f"Stored page {self.pages_processed}/{self.max_pages}: {data['title']}"
            )
        except Exception as e:
            logger.error(f"Error storing content: {e}")

    def extract_links(self, soup, base_url):
        """Extract valid Wikipedia links from page."""
        links = []
        for link in soup.find_all("a"):
            href = link.get("href")
            if href and self.wiki_page_link_pattern.match(href):
                full_url = urljoin(base_url, href)
                if full_url not in self.visited_urls:
                    links.append(full_url)
        return links

    def parse_page(self, url, content):
        """Parse Wikipedia page content."""
        soup = BeautifulSoup(content, "lxml")

        # Extract basic information
        title_elem = soup.find(id="firstHeading")
        if not title_elem:
            return None

        page_data = {
            "url": url,
            "title": title_elem.text.strip(),
            "paragraphs": [],
            "links": [],
            "table_of_contents": [],
        }

        # Extract main content
        content_div = soup.find("div", {"class": "mw-parser-output"})
        if content_div:
            current_section = ""
            for element in content_div.children:
                if element.name == "h2":
                    current_section = element.get_text().strip()
                elif element.name == "p" and element.text.strip():
                    page_data["paragraphs"].append(
                        {"section": current_section, "text": element.text.strip()}
                    )

        # Extract table of contents
        toc = soup.find(id="toc")
        if toc:
            page_data["table_of_contents"] = [
                item.get_text().strip()
                for item in toc.find_all("span", {"class": "toctext"})
            ]

        # Extract and queue new links
        new_links = self.extract_links(soup, url)
        page_data["links"] = new_links
        self.url_queue.extend(new_links)

        return page_data

    def crawl(self):
        """Main crawling method with bulk processing capability."""
        try:
            while self.url_queue and self.pages_processed < self.max_pages:
                # Get next URL to process
                current_url = self.url_queue.popleft()

                # Skip if already visited
                if current_url in self.visited_urls:
                    continue

                logger.info(
                    f"Processing {current_url} ({self.pages_processed + 1}/{self.max_pages})"
                )

                # Download and parse page
                content = self.download_page(current_url)
                if not content:
                    continue

                page_data = self.parse_page(current_url, content)
                if page_data:
                    self.store_page(page_data)
                    self.visited_urls.add(current_url)

                # Save progress periodically
                if self.pages_processed % 100 == 0:
                    self.save_progress()

        except KeyboardInterrupt:
            logger.info("Crawling interrupted by user")
        finally:
            self.save_progress()
            logger.info(
                f"Crawling completed. Total pages processed: {self.pages_processed}"
            )


def run_crawler(config):
    """Function to run a single crawler instance"""
    logger.info(f"Starting crawler for: {config['start_url']}")
    crawler = Wiki_Crawler(
        start_url=config["start_url"],
        directory=config["directory"],
        max_pages=config["max_pages"],
    )
    crawler.crawl()
    logger.info(f"Completed crawling: {config['start_url']}")

def main():
    crawler_configs = [
        {
            "start_url": "https://en.wiktionary.org/wiki/culture",
            "directory": "../pages/culture_wiki",
            "max_pages": 10000,
        },
        {
            "start_url": "https://en.wikipedia.org/wiki/Politics",
            "directory": "../pages/politics_wiki",
            "max_pages": 10000,
        },
        {
            "start_url": "https://en.wikipedia.org/wiki/History",
            "directory": "../pages/history_wiki",
            "max_pages": 10000,
        },
    ]

    # Create and start threads for each crawler
    threads = []
    for config in crawler_configs:
        thread = Thread(target=run_crawler, args=(config,))
        thread.start()
        threads.append(thread)
        logger.info(f"Started thread for: {config['start_url']}")

    # Wait for all crawlers to complete
    for thread in threads:
        thread.join()

    logger.info("All crawlers completed")


if __name__ == "__main__":
    main()
