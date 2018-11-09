from crawler import TrustPilotCrawler 
from time import sleep

cr = TrustPilotCrawler()

cr.begin_crawl(['apple', 'google', 'dsb'])

# Test to see if we can add synonyms dynamically
sleep(5)
cr.add_synonym('samsung')

# Let the thread run until we exit the main thread
while True: 
    pass 