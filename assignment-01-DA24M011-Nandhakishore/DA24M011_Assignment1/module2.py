from .module1 import * 
from bs4 import BeautifulSoup   # type: ignore

class ContentScrapper: 
    __slots__ = '_parser', '_config'
    def __init__(self, page_contents: str, config_file: dict) -> None:
        self._parser = BeautifulSoup(page_contents, 'html.parser')
        self._config = config_file
    
    def find_sub_heading(self, sub_section_str: str = 'sub_heading') -> str: 
        pattern = self._config['webpage'][sub_section_str]
        links = self._parser.find_all('a')

        for link in links: 
            link_text = link.get_text().strip()
            if(pattern.lower() in link_text.lower()): 
                href = link.get('href')
                return self._config['webpage']['base_url'] + href[1:-1]

# -------- Code to test Module2 --------
'''
import yaml
if __name__ == "__main__": 
    page_scrapper = WebPageScrapper(config_file_path = 'config.yaml') 
    webpage_content = page_scrapper.scrape()

    with open('config.yaml', 'r') as file: 
        config = yaml.safe_load(file)
    
    topnews_scrapper = ContentScrapper(page_contents = webpage_content, config_file = config) 
    top_news_url = topnews_scrapper.find_sub_heading()
    print(f'Top news URL: \n{top_news_url}\n')
'''
