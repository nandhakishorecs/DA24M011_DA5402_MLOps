import requests     # type: ignore
import bs4          # type: ignore
from bs4 import BeautifulSoup       # type: ignore
import time         
import json
from datetime import datetime, timedelta

from .module1 import *
from .module2 import *


class ContentExtractor:
    __slots__ = '_config', '_headers'
    def __init__(self, config_file: dict) -> None:
        self._config = config_file
        self._headers = {
            'User-Agent': config_file['webpage']['user_agent']
        }

    def extract_stories(self, url: str, max_retries: int = 2) -> list:
        stories = []
        page = 1
        
        while(page <= max_retries):
            try:
                response = requests.get(url, headers=self._headers)
                response.raise_for_status()
                soup = BeautifulSoup(response.text, 'html.parser')
                articles = soup.find_all('article')
                
                for article in articles:
                    story = self._extract_story_data(article)
                    if story and story not in stories:
                        stories.append(story)
                
                if ((not articles) or (len(articles) == 0)):
                    break
                
                page += 1
                time.sleep(5)
                
            except requests.RequestException as error:
                print(f"Error fetching page {page}: {error}")
                break
                
        return stories

    def _get_time(self, relative_time: str) -> str:
        try:
            now = datetime.now()
            
            if 'minute' in relative_time or 'minutes' in relative_time:
                minutes = int(relative_time.split()[0])
                return (now - timedelta(minutes=minutes)).strftime("%Y-%m-%d %H:%M:%S")
            
            elif 'hour' in relative_time or 'hours' in relative_time:
                hours = int(relative_time.split()[0])
                return (now - timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")
            
            elif 'day' in relative_time or 'days' in relative_time:
                days = int(relative_time.split()[0])
                return (now - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
            
            elif 'Yesterday' in relative_time:
                return (now - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
            
            else:
                return now.strftime("%Y-%m-%d %H:%M:%S")
                
        except Exception:
            return now.strftime("%Y-%m-%d %H:%M:%S")

    def _extract_story_data(self, article: bs4.element.Tag) -> dict:
        try:
            # Extract headline and URL first
            headline = None
            for a_tag in article.find_all('a'):
                if(a_tag.get_text().strip() is not None):
                    headline = a_tag.get_text().strip()
                    article_url = a_tag.get('href')
                    if((article_url) and (not article_url.startswith('http'))):
                        # article_url = f"https://news.google.com{article_url[1:-1]}"
                        article_url = self._config['webpage']['base_url'] + article_url[1:-1]
                    else: 
                        break
            
            if(not headline):
                return None
            
            # Extract thumbnail
            thumbnail_url = None

            # Try figure tag first
            figure = article.find('figure')
            if(figure is not None):
                img = figure.find('img')
                if(img is not None):
                    thumbnail_url = img.get('src') or img.get('data-src')
                    # thumbnail_url = f"https://news.google.com{thumbnail_url}"
                    thumbnail_url = self._config['webpage']['base_url'] + thumbnail_url
            
            # If no image found, skip this story
            if(not thumbnail_url): return
            
            # Extract and convert publication date
            time_elem = article.find('time')
            relative_time = time_elem.get_text().strip() if time_elem else None
            pub_date = self._get_time(relative_time) if relative_time else datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            return {
                'Headline': headline,
                'Thumbnail_url': thumbnail_url,
                'Article_url': article_url,
                'Date': pub_date
            }
            
        except Exception as error:
            print(f"Error:\t {error}")
            return

    def save_stories(self, content: str, filename: str) -> None:
        filename += '.json'
        with open(filename, 'w', encoding = 'utf-8') as f:
            json.dump(content, f, indent = 2, ensure_ascii = False)

# -------- Code to test Module 3 --------
'''
import yaml
if __name__ == "__main__":

    with open('config.yaml', 'r') as file:
        config_file = yaml.safe_load(file)

    scraper = WebPageScrapper(config_file_path = 'config.yaml')
    homepage_content = scraper.scrape()
    
    if(homepage_content is not None):
        sub_section_scraper = ContentScrapper(page_contents = homepage_content, config_file = config_file)
        sub_section_link = sub_section_scraper.find_sub_heading()
        
        if(sub_section_link is not None):
            extractor = ContentExtractor(config_file)
            stories = extractor.extract_stories(sub_section_link)
            if stories:
                extractor.save_stories(content = stories, filename='Stories')
                print(f"\nSuccessfully extracted {len(stories)} stories to stories.json\n")
            else:
                print("\nNo stories in the given subsection found\n")
        else:
            print("\nGiven url is invalid\n")
'''