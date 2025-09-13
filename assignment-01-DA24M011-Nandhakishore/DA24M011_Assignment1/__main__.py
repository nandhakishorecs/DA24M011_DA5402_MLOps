from .module1 import *
from .module2 import *
from .module3 import *
from .module4 import *
from .module5 import *
from .module6 import *

import sys

if __name__ == "__main__":
    # -------- Code to check for duplicates as a separate module --------
    '''
    with open('config.yaml', 'r') as file:
        config = yaml.safe_load(file)
    
    checker = DuplicationChecker(config)

    test_story = {
        "Headline": "Will income tax booster by Modi government safeguard stock market bulls against FII selloff?",
        "Thumbnail_url": "https://news.google.com/api/attachments/CC8iK0NnNVVOVk0xZFhBeVZUZzFkVnBKVFJDZkF4ampCU2dLTWdZQkFKek9FUXM=-w280-h168-p-df",
        "Article_url": "https://news.google.com/read/CBMi8wFBVV95cUxNWktIX3EyTkNZQ2J2WDhBa3FPMWtZX1MydXlUU01nWGd6a1g2WTAwX1NGRUFVSk5CdVZ3UFQtZ0U5YXVXVGJ2bDZOQWdVMW9fZUxBMHdIWkw4QXE1YnhtS09EQ0xtR0JVSWhWNHRaTlNUWXcwbnJEdlpnd3BHZ2NWTkFjczE4anNkM1UzR25OdFVzVFI3eXdoQ3RCQjdPckhEWXdZNTRscnJBWHZfckxYZkFUZEg2YXVPWWpPcG9aTUdMT2VOYlpybEEzdXRlMGtYelBVTGhPaFN3ZXZGekFFbkJWaDJoVDlOZ0NtN0pkeXFWZzDSAfgBQVVfeXFMTy1Bb0FXRE5GbGNDbmtabml1VmpIdmpteTd0ZkVJZkY0Ump2U1U5XzhrNEN6MjNGdklJZzBCN1lhZlpvRmVwVkYydUdUMTAwTXF1ZFd0VE5TcE1RRWNmUTNiUjFucUp4RWVFWW5wX1ZUTDE4M0RXMHVETDlwZ0w5MjdTTV9Ra0FDSHV4SGQ4VUV0LTI1X0tpVTAyazdZdUtoZFp6d2JtMTdoNEd6a0YyTGdJa3ZRT3RmSzl0eHNiNTdjZEhHUDJVUW1FWHpNc1hpUTJPNDFFTHZjZHNONDVxOWh2MHNsdnNtT0licTlvRW9wX19FUXdRakw?hl=en-IN&gl=IN&ceid=IN%3Ae",
        "Date": "2025-02-01 20:43:01"
    }
    test_story = {
        "Headline": "Focus on Bihar: Here are the schemes announced for the state in the Budget",
        "Thumbnail_url": "https://news.google.com/api/attachments/CC8iL0NnNUJUbGgxVUUxVVN6aHdjbHAxVFJDYkF4anFCU2dLTWdrQk1KSWhHMm9XeXdF=-w280-h168-p-df",
        "Article_url": "https://news.google.com/read/CBMi3wFBVV95cUxOdjRiMlBTTk5MVi1HN0ZWeXZIeGw4UnBjM2dETjFfUXl3TkJtb3MwSEZPMmtGUEFKY2xubWI4TW9Uc1NsMWVFRmxTNmRZZWpaOUlyekVIZURrb3YyMld4S1lvTGxzam9mSFlEUERUSm5JTGNpQk51Z0hxaElWenZ1cUYyOEZlSjhLd1A3dHNmaUdUd2Z6T24zaVRxa2FKcnZPSng1ckRQSGM1Y21ta2Q0U3Nkdl90SXJ4VWpzcmtDS3k5ODQySE1uM3UzT2Vqb1hlUDJ2UUJEZlpmRjdpcGtV0gHmAUFVX3lxTE1PTVlsbWlqVVhSVmdVbzBteUdqWE4xRzB0RGhMbmhxRnpmektsUEVMbUd4Ni13TGhObUlnYTd1aWF4cURCRVlkVnpVb0NyRW4zbzd2SU8wcmJ3V0RIWG9xb0YwZU1Xc0VMN25KMkNKLTkycXFfM1MwT0FuVmdaYWNKbGNEcWZLTnJRaVVId01JeE9hc2lzekFUOUpTYXZaRG1LUnlYQl9sUDNGdFdKWmI2MmJrUXgwTEh6YkhvUERfYV9GNjBKRVNsd0Z1YlBuZG9XUkZYdVQzYzBVTUR2LWFWVmtaZV9B?hl=en-IN&gl=IN&ceid=IN%3Ae",
        "Date": "2025-02-01 21:43:01"
    }
    
    is_duplicate, reason = checker.is_duplicate(test_story)
    print(f"Reason: {reason}")
    
    checker.close()
    '''
    print('\nDA5402\tMachine Learning Operations Lab - Assignment 1\n')
    print('\nSibmitted by:\t Nandhakishore C S\n')

    setup_logging()
    
    try:
        with open('/Users/nandhakishorecs/Documents/IITM/Jan_2025/DA5402/submissions/assignment-01-DA24M011-Nandhakishore/DA24M011_Assignment1/config.yaml', 'r') as file:
            config = yaml.safe_load(file)
        
        frequency = config.get('pipeline', {}).get('frequency', )  


        while True:
            run_pipeline(config)
            logging.info(f"\nWaiting {frequency} seconds until next run...\n")
            logging.info(f"Press Ctrl + C to stop\n")
            time.sleep(frequency)

    except KeyboardInterrupt:
        logging.info("\nPipeline stopped by user\n")
    except Exception as error:
        logging.error(f"\nFatal error:\t{str(error)}\n")
        sys.exit(1)
    