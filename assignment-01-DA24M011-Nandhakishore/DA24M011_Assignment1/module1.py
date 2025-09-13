import yaml     # type: ignore
import requests # type: ignore

class WebPageScrapper: 
    __slots__ = '_config_file'
    def __init__(self, config_file_path: str) -> None:
        if(config_file_path): 
            self._config_file = self._load_config(config_file_path)
        else: 
            self._config_file = None
    
    def _load_config(self, config_file_path: str):
        with open(config_file_path, 'r') as file: 
            return yaml.safe_load(file)
        
    def scrape(self): 
        url = self._config_file['webpage']['base_url']
        responce = requests.get(url)
        responce.raise_for_status() 
        return responce.text
    
# -------- Code to test module 1 --------
'''
if __name__ == '__main__': 
    website_scrapper = WebPageScrapper(config_file_path = 'config.yaml') 
    content = website_scrapper.scrape()
    print(f'Website Content:\n{content}\n')
'''