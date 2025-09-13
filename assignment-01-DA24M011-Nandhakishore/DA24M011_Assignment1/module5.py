import psycopg2 as psycopg      # type: ignore
import logging
from difflib import SequenceMatcher

class DuplicationChecker:
    __slots__ = '_connection', '_db_configuration', '_cursor', '_log'
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
        self._log = logging.getLogger('DuplicateChecker')

    def check_similarity(self, headline1: list, headline2: list) -> float:
        return SequenceMatcher(None, headline1.lower(), headline2.lower()).ratio()

    def is_duplicate(self, story: list, same_day_threshold: float = 0.85, different_day_threshold: float = 0.95):
        try:
            story_date = story['Date'][:10]  # Extract YYYY-MM-DD
            
            # First check same day articles with lower threshold
            self._cursor.execute(
                """
                    SELECT publication_date, headline
                    FROM news_articles
                    WHERE SUBSTRING(publication_date, 1, 10) = %s
                """, 
                (story_date,)
            )
            
            same_day_stories = self._cursor.fetchall()
            
            for existing_headline, _ in same_day_stories:
                similarity = self.check_similarity(story['Headline'], existing_headline)
                if(similarity >= same_day_threshold):
                    self._log.info(f"Duplicate found - Headline: {story['Headline']} - Same day duplicate found - Similarity: {similarity:.2f}")
                    return True, f"Same day duplicate found - Similarity: {similarity:.2f}"
            
            # Then check other days with higher threshold
            self._cursor.execute("""
                    SELECT headline, publication_date 
                    FROM news_articles
                    WHERE SUBSTRING(publication_date, 1, 10) != %s
                """, 
                (story_date,)
            )
            
            different_day_stories = self._cursor.fetchall()
            
            for existing_headline, _ in different_day_stories:
                similarity = self.check_similarity(story['Headline'], existing_headline)
                if( similarity >= different_day_threshold):
                    self._log.info(f"Duplicate found - Headline: {story['Headline']} - Different day duplicate found - Similarity: {similarity:.2f}")
                    return True, f"Different day duplicate found - Similarity: {similarity:.2f}"
                
            return False, "No duplicate found"
            
        except Exception as error:
            self._log.error(f"Error checking duplication: {error}")
            return False, f"Error: {str(error)}"

    def close(self):
        self._cursor.close()
        self._connection.close()

# -------- Code to test Module 5 --------
'''
import yaml
if __name__ == "__main__":
    with open('config.yaml', 'r') as file:
        config = yaml.safe_load(file)
    
    checker = DuplicationChecker(config)
    
    test_story ={
        "Headline": "Silicon Valley consortium values London Spirit at £295 million in Hundred coup",
        "Thumbnail_url": "https://news.google.com/api/attachments/CC8iL0NnNUpVemhzVWpoRVRTMXZUVjlLVFJDZkF4ampCU2dLTWdrQlVJeEJ2YVV0a2dF=-w280-h168-p-df",
        "Article_url": "https://news.google.com/read/CBMivAFBVV95cUxNSWV1TWdWNThDZnYxbE8wdDA5NXZoZHdIX1FoT3J2SXFlc2R6ZUdFRjRlUTlGcUdFMmVBTTE4Yk5LMjlORUhYUkExWXVEeTZUM2drQWtURmxLdnF5alNrOTlMTFhwYVBVNmJMb2JRY3hfNk1wdENBTGJDZjJxZ2ZKcDI0bExCNlBJd201dncxajRnakJ3RGh1UjZkaHZRQVRDeFpsWFVaZ3hiTFlKbW1qUDQ4bTlDVmhyUlFNadIBzgFBVV95cUxObmU4YXd5dGFiUWRfWTM4a3REMktJVlNDYzR3MzlfVGV1Rll5am83OGJiR1VPNmZrRjRlVkRUUkl1c0Z4MVNnTS00SG5PZTVfbExGWWs4cHhKN1RXeVI0UXNDUUhfQ2Nmck5ERk5qZVl6cnZEbDVGYlo0UmY3bks3Q3lzM0lNQVVTSjRVUGZZVS0wUG4yakNyQlBMMlc1dUJaR05sdWdXVjR0blBwRWRrTmR6YlBhR1h1eVBCQkM3WEhpdlNYZkFEVGUyVXhuQQ?hl=en-IN&gl=IN&ceid=IN%3Ae",
        "Date": "2025-02-01 01:52:08"
    }


    # test_story = {
    #     'Headline': 'Maha Kumbh stampede LIVE updates: At least 30 dead and 60 injured, says DIG Vaibhav Krishna',
    #     'Thumbnail_url':'ijj',
    #     'Article_url': 'https://news.google.com/read/abc123',
    #     'Date': '2025-01-29 17:54:49'
    # }
    
    is_duplicate, reason = checker.is_duplicate(test_story)
    print(f"Reason: {reason}")
    
    checker.close()
'''