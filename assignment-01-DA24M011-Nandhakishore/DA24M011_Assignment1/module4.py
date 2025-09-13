import requests     # type: ignore
import logging
import psycopg2 as psycopg  # type: ignore

from .module1 import *
from .module2 import *
from .module3 import *
from .module5 import *

class NewsDB:
    __slots__ = '_connection', '_db_configuration', '_cursor', '_log', '_duplicate_checker'
    def __init__(self, config_file: dict) -> None:
        self._db_configuration = config_file['database']
        self._connection = psycopg.connect(
            dbname      = self._db_configuration['dbname'],
            user        = self._db_configuration['user'],
            password    = self._db_configuration['password'],
            host        = self._db_configuration['host'],
            port        = self._db_configuration['port']
        )
        self._cursor = self._connection.cursor()
        
        # Setup logging
        logging.basicConfig(
            filename    = 'duplicates.log',
            level       = logging.INFO,
            format      = '%(asctime)s - %(message)s',
            datefmt     = '%Y-%m-%d %H:%M:%S'
        )
        self._log = logging.getLogger('DuplicateChecker')
        self._duplicate_checker = DuplicationChecker(config_file)

    def store_article(self, story: list):
        try:
            # Check for duplicates first
            is_duplicate, reason = self._duplicate_checker.is_duplicate(story)
            
            if(is_duplicate):
                self._log.info(f"Duplicate found - Headline: {story['Headline']} - {reason}")
                return False

            # Insert article data if not duplicate
            self._cursor.execute(
                """
                    INSERT INTO news_articles (headline, article_url, publication_date)
                    VALUES (%s, %s, %s)
                    RETURNING id
                """, 
                (story['Headline'], story['Article_url'], story['Date'])
            )
            
            article_id = self._cursor.fetchone()[0]
            
            # Store image if exists
            if (story['Thumbnail_url'] is not None):
                try:
                    response = requests.get(story['Thumbnail_url'])
                    image_data = response.content
                    
                    self._cursor.execute(
                        """
                            INSERT INTO news_images (article_id, thumbnail_url, image_data)
                            VALUES (%s, %s, %s)
                        """, 
                        (article_id, story['Thumbnail_url'], psycopg.Binary(image_data))
                    )
                except Exception as error:
                    self._log.error(f"Error acquiring image: {error}")
            
            self._connection.commit()
            return True
            
        except Exception as error:
            self._connection.rollback()
            self._log.error(f"Error storing article: {error}")
            return False

    def close(self):
        try:
            if self._cursor:
                self._cursor.close()
            if self._connection:
                self._connection.close()
            if hasattr(self, 'duplicate_checker'):
                self._duplicate_checker.close()
        except Exception as error:
            self._log.error(f"Error during cleanup: {error}")

# ---------------- Code to test Module 4 ----------------
'''
import yaml
if __name__ == "__main__":
    with open('config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    # Get stories using previous modules
    home_scraper = WebPageScrapper('config.yaml')
    homepage_content = home_scraper.scrape()
    
    if homepage_content:
        top_stories_scraper = ContentScrapper(homepage_content, config)
        top_stories_url = top_stories_scraper.find_sub_heading()
        
        if top_stories_url:
            story_extractor = ContentExtractor(config)
            stories = story_extractor.extract_stories(top_stories_url)
            
            if stories:
                # Store in database
                db = NewsDB(config)
                stored_count = 0
                skipped_count = 0
                
                for story in stories:
                    if db.store_article(story):
                        stored_count += 1
                    else:
                        skipped_count += 1
                
                db.close()
                db._log.info(f"Successfully stored {stored_count} articles in database")
                db._log.info(f"Skipped {skipped_count} articles")
            else:
                logging.info("No stories were found")
        else:
            logging.info("Could not find top stories link")
'''