from post_retriever import PostRetriever
from urllib.request import urlopen, urlparse
from bs4 import BeautifulSoup as bs 
from queue import Queue
import time 
from time import sleep

class TrustPilotCrawler(PostRetriever):
    """ 
    Simple single-host crawler that extract company reviews from Trustpilot. 
    Synonyms can be added dynamically. Performs a Trustpilot search for each synonym 
    and extracts reviews for all query results.
    Reviews are dynamically saved to a database. 
    Reviews for a given synonym can be fetched at any time. When synonyms have been fetched 
    successfully, they are removed from the database. 
    """

    def __init__(self): 
        self.synonyms = []

        # The synonym queue is a queue of dictionaries: 
        # { synonym : Queue(URL) }
        # When a synonym is popped from the queue, the crawler 
        # pops a URL from the synonym's queue. All resulting URLs
        # from the URLs webpage are enqueued in the synonym's URL queue. 
        self.synonym_queue = Queue() 

        #TODO: Maintain a list of review IDs so we can make sure we don't 
        #      process the same review twice.  

        self.host_timer = time.time() 

    def add_synonym(self, synonym): 
        """ 
        Adds a synonym to the list of tracked synonyms. 
        Performs a Trustpilot search for the synonym. 
        All query result links are stored in the synonym's queue, 
        both of which are added to the synonym queue. 
        """
        review_pages = self._search_for_synonym(synonym)
        if len(review_pages) == 0: 
            pass # Maybe handle this in another way? 

        # Enqueue all pages for this synonym 
        url_queue = Queue() 
        for page in review_pages: 
            url_queue.put(page)
        
        # Add to the global synonym queue
        self.synonym_queue.put({synonym : url_queue})

    def can_ping_yet(self): 
        now = time.time() 
        return now - self.host_timer > 2

    def _search_for_synonym(self, synonym): 
        """
        Performs a Trustpilot search for the synonym. 
        Returns all resulting URLs in a list. 
        """
        soup = self._get_souped_page(f'https://www.trustpilot.com/search?query={synonym}')
        review_pages = soup.findAll("a", {"class": "search-result-heading"}, href = True)
        return [f'https://www.trustpilot.com{page["href"]}' for page in review_pages]

    def _get_next_synonym(self): 
        """ 
        Returns the next {synonym : Queue(URL)} dict in the queue.
        """
        return self.synonym_queue.get()

    def _get_url_queue_from_synonym(self, synonym_dict): 
        """ 
        Given a {synonym : Queue(URL)} dict, returns the Queue for the synonym.
        """
        return list(synonym_dict.values())[0]

    def _get_souped_page(self, url): 
        """ 
        Gets the webpage pointed to by the URL as a parsed 
        BeatifulSoup object. 
        NOTE: Always use this method when downloading Trustpilot 
        webpages, as it ensures (time) politeness.
        """
        # Check if we can ping Trustpilot yet 
        while not self.can_ping_yet(): 
            sleep(0.05) # Wait for 50 milliseconds

        page = urlopen(url)
        self.host_timer = time.time()
        print(f'Downloaded: {url}')
        return bs(page, features='html5lib')

    def _process_review_page(self, review_page_url): 
        """ 
        Takes a URL for a Trustpilot Review page and downloads it. 
        After downloading, it extracts all available review texts and returns them. 
        It also returns the "Next page" link if it exists.
        """
        soup = self._get_souped_page(review_page_url)
        reviews = soup.findAll('section', {'class' : 'content-section__review-info'})
        
        review_contents = [{'title'  : review.find('h2',  {'class', 'review-info__body__title'}).get_text().strip(),
                            'body'   : review.find('p',   {'class', 'review-info__body__text'}).get_text().strip()
                            } for review in reviews]
        
        return review_contents





        


    


