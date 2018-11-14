from crawler import TrustPilotCrawler 
from time import sleep

cr = TrustPilotCrawler()


# TODO: Bind the following to a "begin_crawl" endpoint.
# To begin the crawler (should really only be done once), enter seed URLs like this:
cr.begin_crawl(['apple', 'google', 'dsb'])
# ... or by using the add_synonym method before calling begin_crawl.


# TODO: Bind the following to a "track_new_synonym" endpoint.
# When begin_crawl has been called, the crawler is running in a separate thread. 
# The thread accesses a global synonym queue in the main thread that we can add elements 
# to whenever we want like this: 
sleep(5)
cr.add_synonym('samsung')

# TODO: Bind the following to a "get_crawled_data" enpoint. 
# Use the following method to retrieve all text data that has been crawled for a given
# synonym up to this point. 


# TODO: Bind the following to an "end_crawl" endpoint.
# Let the thread run until we exit the main thread. 
# The thread is Matt Daemon so it will terminate when this thread terminates. 
while True: 
    pass
