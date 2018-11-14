from post_retriever import PostRetriever
from urllib.request import urlopen, urlparse
from bs4 import BeautifulSoup as bs 
from queue import Queue
import time 
from time import sleep
from threading import Thread
import traceback
from datetime import datetime
# Database
import database
from database import session, Synonym, Post, SynonymPostAssociation
from sqlalchemy.orm import joinedload

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

        self.host_timer = time.time() 
        self.crawled_data = {}
        self.synonyms = set() 
        self.seen_reviews = {}

    def begin_crawl(self, synonyms = None):
        if not synonyms == None: 
            self.add_synonyms(synonyms)
        
        crawler_thread = Thread(target = self._threaded_crawl, args=[self.synonym_queue, self.crawled_data], daemon = True)
        crawler_thread.start()
         

    def _threaded_crawl(self, queue, data_store):
        while True: 
            try: 
                # Get the next synonym dict in the queue 
                synonym_urls = queue.get()
                # Pop the first link in the URL queue
                url_queue = self._get_url_queue_from_synonym(synonym_urls)
                synonym = self._get_synonym_from_synonym_dict(synonym_urls)
                
                if url_queue.empty(): 
                    # The queue should be restarted from the initial Trustpilot search. 
                    self.add_synonym(synonym)
                    # Skip the rest of this loop
                    continue

                url = url_queue.get()
                # Get reviews from this URL
                print(f'Processing: {url}')
                print(f'Currently looking at synonyms: {self.synonyms}')
                print(f'Number of entries in data store: {len(data_store.values())}')
                print('-------------------------------------------------------------------------------------')

                reviews, next_page = self._get_reviews_from_url(review_page_url = url)
                # Store the extracted reviews 
                already_seen = False
                for review in reviews: 
                    # Only save the review if we have not seen it
                    already_seen = self.ensure_data_store(synonym, review)
                    if already_seen: 
                        # Break out of the for loop.
                        break

                # Requeue the synonym dict 
                if (not next_page == None) and not already_seen: 
                    url_queue.put(next_page)

                queue.put(synonym_urls)
                # TODO: Check for scheduled data dump 
                # TODO: Dump data in local database every once in a while. 
                #       Allow the database to be accessed from anywhere.  

            except Exception as e: 
                print(f'Exception encountered in crawling thread: {e}')
                traceback.print_exc()
                return

    def add_synonyms(self, synonyms):
        for synonym in synonyms:
            self.add_synonym(synonym)

    def add_synonym(self, synonym): 
        """ 
        Adds a synonym to the list of tracked synonyms. 
        Performs a Trustpilot search for the synonym. 
        All query result links are stored in the synonym's queue, 
        both of which are added to the synonym queue. 
        """
        review_pages = self._get_synonym_review_pages(synonym)
        if len(review_pages) == 0: 
            pass # Maybe handle this in another way? 

        # Enqueue all pages for this synonym 
        url_queue = Queue() 
        for page in review_pages: 
            url_queue.put(page)
        
        # Add to the global synonym queue
        self.synonym_queue.put({synonym : url_queue})
        self.synonyms.add(synonym)

    def can_ping_yet(self): 
        now = time.time() 
        # Return boolean as well as time remaining
        return now - self.host_timer > 2, 2 - (now - self.host_timer)

    def _get_synonym_review_pages(self, synonym): 
        """
        Performs a Trustpilot search for the synonym. 
        Returns all relevant URLs in a list. 
        """
        soup = self._get_souped_page(f'https://www.trustpilot.com/search?query={synonym}')
        review_pages = soup.findAll("a", {"class": "search-result-heading"}, href = True)
        
        return [f'https://www.trustpilot.com{page["href"]}' for page in review_pages if self._is_relevant_review_page(synonym, page.get_text())]

    def _is_relevant_review_page(self, synonym, link_text): 
        """
        Relevant review pages are linked to with the text (query for "Google"):
            "Google | www.google.com"
        A non-relevant link could be: 
            "Google Adwords | www.adwords.google.com"
        This function returns true if the link text contains a "|", and the 
        left hand side of the pipe symbol is exactly the synonym.
        """

        return '|' in link_text and (synonym == link_text.split('|')[0].lower().strip())

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

    def _get_synonym_from_synonym_dict(self, synonym_dict): 
        """ 
        Given a {synonym : Queue(URL)} dict, returns the synonym.
        """
        return list(synonym_dict.keys())[0]

    def _get_souped_page(self, url): 
        """ 
        Gets the webpage pointed to by the URL as a parsed 
        BeatifulSoup object. 
        NOTE: Always use this method when downloading Trustpilot 
        webpages, as it ensures (time) politeness.
        """
        # Check if we can ping Trustpilot yet 
        can_ping, remaining_time = self.can_ping_yet()
        if not can_ping: 
            sleep(remaining_time)

        page = urlopen(url)

        # Reset timer before returning the souped page
        self.host_timer = time.time()
        return bs(page, features='html5lib')

    def _get_reviews_from_url(self, review_page_url): 
        """ 
        Takes a URL for a Trustpilot Review page and downloads it. 
        After downloading, it extracts all available review texts and returns them. 
        It also returns the "Next page" link if it exists.
        """

        soup = self._get_souped_page(review_page_url)
        cards = soup.findAll('section', {'class', 'review-card__content-section'})
        users = [card.find('h3', {'class', 'consumer-info__details__name'}).get_text() for card in cards]
        reviews = soup.findAll('section', {'class' : 'content-section__review-info'})

        next_page = self._get_next_page(soup)
        
        return [
            {
                'title'  : review.find('h2',  {'class', 'review-info__body__title'}).get_text().strip(),
                'body'   : review.find('p',   {'class', 'review-info__body__text'}).get_text().strip(),
                'date'   : self._get_date(review),
                'user'   : user.strip()
            } 
            for (review, user) in zip(reviews, users)], next_page

    def _get_next_page(self, souped_review_page): 
        next_page = souped_review_page.find('a', {'class', 'pagination-page next-page'}, href = True)
        if next_page == None: 
            return None 
        return f'https://www.trustpilot.com{next_page["href"]}'

    def _get_date(self, review): 
        date = review.find('div', {'class', 'header__verified__date'})
        date = date.find('time')['datetime']
        return date

    def ensure_data_store(self, synonym, review): 
        """
        Returns true if the review has been seen, otherwise false.
        Takes a review an verifies whether or not this review has been seen before. 
        This is determined by looking at the date and the user. If it has been seen, 
        the function does nothing. Otherwise, an entry is made to the seen_reviews 
        dictionary with this user and date.
        """
        if synonym not in self.crawled_data.keys(): 
            self.crawled_data[synonym] = []

        user = review['user']
        date = review['date'].split('T')[0]
        if synonym not in self.seen_reviews.keys():
            # We have not even seen the synonym before, so set up its dict structure.
            self.seen_reviews[synonym] = {date : [user]}
            self.commit_review(synonym, review)
            return False
        elif date not in self.seen_reviews[synonym]:
            # This date has not been created, so we have not seen it either. 
            self.seen_reviews[synonym][date] = [user]
            self.commit_review(synonym, review)
            return False
        elif user not in self.seen_reviews[synonym][date]: 
            # We have not seen this user post today, so add the review. 
            self.commit_review(synonym, review)
            return False
        
        # Otherwise, simply return that we have already seen it
        print(f'Found a duplicate! {user} at {date}')
        return True

    def _clear_data_store(self): 
        session.query(Synonym).delete()
        session.query(Post).delete()
        session.query(SynonymPostAssociation).delete()
        session.commit()
        print(f'Sucessfully deleted all data from DB.')


    def dump(self, delete_data = False):
        """
        Dumps all stored data for every synonym to the caller. 
        Also returns a function that, when called, empties the 
        local data store. 
        """
        synonyms = session.query(Synonym).options(joinedload('posts')).all()
        if delete_data: 
            self._clear_data_store()
        return synonyms

    def commit_review(self, synonym, review): 
        """
        Commits a synonym <--> post relation to the database. 
        """
        existing_synonyms = [synonym.name for synonym in session.query(Synonym)]
        synonym_exists = synonym in existing_synonyms

        # Post attributes  
        date = review['date'].split('T')[0]
        date = datetime.strptime(date, "%Y-%m-%d")
        contents = f"{review['title']}. {review['body']}"

        oPost = Post(date = date, contents = contents)

        # Check if synonym exists 
        if synonym_exists: 
            oSyn = session.query(Synonym).filter_by(name = synonym).first()
            oSyn.posts.append(oPost)

        else: 
            oSyn = Synonym(name = synonym)
            oSyn.posts.append(oPost)
            session.add(oSyn)
            print(f'Adding {synonym} to database.')

        session.commit()

        






        





        


    


