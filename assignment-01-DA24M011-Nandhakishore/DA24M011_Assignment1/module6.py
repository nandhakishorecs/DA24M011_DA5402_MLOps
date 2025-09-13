import logging
from datetime import datetime
import traceback
from .module1 import *
from .module2 import *
from .module3 import *
from .module4 import *

def setup_logging() -> None:
    logging.basicConfig(
        filename    = 'pipeline.log',
        level       = logging.INFO,
        format      = '%(asctime)s - %(levelname)s - %(message)s',
        datefmt     = '%Y-%m-%d %H:%M:%S'
    )
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    logging.getLogger().addHandler(console_handler)

def run_pipeline(config):
    try:
        start_time = datetime.now()
        logging.info("\nStarting news scrapping pipeline...\n")
        
        # Module 1: Scrape homepage
        home_scraper = WebPageScrapper('/Users/nandhakishorecs/Documents/IITM/Jan_2025/DA5402/submissions/assignment-01-DA24M011-Nandhakishore/DA24M011_Assignment1/config.yaml')
        homepage_content = home_scraper.scrape()
        if(not homepage_content):
            raise Exception("\nFailed to scrape webpage\n")
        
        # Module 2: Extract top stories link
        top_stories_scraper = ContentScrapper(homepage_content, config)
        top_stories_url = top_stories_scraper.find_sub_heading()
        if not top_stories_url:
            raise Exception("\nFailed to find top stories url\n")
        
        # Module 3: Extract stories
        story_extractor = ContentExtractor(config)
        stories = story_extractor.extract_stories(top_stories_url)
        if not stories:
            raise Exception("\nNo stories were extracted\n")
        
        # Module 4: Store in database
        db = NewsDB(config)
        stored_count = 0
        skipped_count = 0
        
        for story in stories:
            if db.store_article(story):
                stored_count += 1
            else:
                skipped_count += 1
        
        db.close()
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logging.info(f"\nPipeline completed. Stored: {stored_count}, Skipped: {skipped_count}, Duration: {duration:.2f}s\n")
        return True
        
    except Exception as error:
        logging.error(f"\nPipeline failed:\t{str(error)}\n")
        logging.error(traceback.format_exc())
        return False

# -------- Code to test Module 6 --------
'''
import yaml
import time

if __name__ == "__main__":
    setup_logging()
    
    try:
        with open('config.yaml', 'r') as file:
            config = yaml.safe_load(file)
        
        frequency = config.get('pipeline', {}).get('frequency', )  # Default: .5 hour in seconds
        
        while True:
            run_pipeline(config)
            logging.info(f"Waiting {frequency} seconds until next run...")
            time.sleep(frequency)
            
    except KeyboardInterrupt:
        logging.info("Pipeline stopped by user")
    except Exception as error:
        logging.error(f"Fatal error: {str(error)}")
        sys.exit(1)
'''