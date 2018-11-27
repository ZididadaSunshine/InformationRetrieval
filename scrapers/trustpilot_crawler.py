import time
import traceback
from datetime import datetime
from queue import Queue
from threading import Thread
from time import sleep
from urllib.request import urlopen
from KeywordExtraction.preprocessing.text_preprocessing import get_processed_text

from bs4 import BeautifulSoup as bs

from dbhandler import DBHandler


class TrustPilotCrawler:
    """ 
    Simple single-host crawler that extract company reviews from Trustpilot. 
    Synonyms can be added dynamically. Performs a Trustpilot search for each synonym 
    and extracts reviews for all query results.
    Reviews are dynamically saved to a database. 
    Reviews for a given synonym can be fetched at any time. When synonyms have been fetched 
    successfully, they are removed from the database. 
    """

    def __init__(self):
        self.db = DBHandler()
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

    def begin_crawl(self, synonyms=None, verbose=False):
        if synonyms is not None:
            self.add_synonyms(synonyms)

        crawler_thread = Thread(target=self._threaded_crawl, args=[self.synonym_queue, verbose], daemon=True)
        crawler_thread.start()

    def _threaded_crawl(self, queue, verbose=False):
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
                if verbose:
                    print(f'Processing: {url}')
                    print(f'Currently looking at synonyms: {self.synonyms}')
                    print('-------------------------------------------------------------------------------------')

                reviews, next_page = self._get_reviews_from_url(review_page_url=url)
                # Store the extracted reviews
                for review in reviews:
                    self._process_entry(synonym, review)

                # Requeue the synonym dict 
                if next_page is not None:
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
            pass  # Maybe handle this in another way?

        # Enqueue all pages for this synonym 
        url_queue = Queue()
        for page in review_pages:
            url_queue.put(page)

        # Add to the global synonym queue
        self.synonym_queue.put({synonym: url_queue})
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
        review_pages = soup.findAll("a", {"class": "search-result-heading"}, href=True)

        return [f'https://www.trustpilot.com{page["href"]}' for page in review_pages if
                self._is_relevant_review_page(synonym, page.get_text())]

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
        reviews = soup.findAll('section', {'class': 'content-section__review-info'})
        users_review_counts = zip(
            [card.find('h3', {'class', 'consumer-info__details__name'}).get_text() for card in cards],
            [card.find('span', {'class', 'consumer-info__details__review-count'}).get_text()
                 .strip().split(' ')[0] for card in cards]
        )

        next_page = self._get_next_page(soup)

        return [
                   {
                       'title': review.find('h2', {'class', 'review-info__body__title'}).get_text().strip(),
                       'body': review.find('p', {'class', 'review-info__body__text'}).get_text().strip(),
                       'date': self._get_date(review),
                       'user': user.strip(),
                       'review_count': review_count
                   }
                   for (review, (user, review_count)) in zip(reviews, users_review_counts)], next_page

    def _get_next_page(self, souped_review_page):
        next_page = souped_review_page.find('a', {'class', 'pagination-page next-page'}, href=True)
        if not next_page:
            return None
        return f'https://www.trustpilot.com{next_page["href"]}'

    def _get_date(self, review):
        date = review.find('div', {'class', 'header__verified__date'})
        date = date.find('time')['datetime']
        return date

    def _process_entry(self, synonym, review):
        """
        Commits a synonym <--> post relation to the database. 
        """

        # Post attributes  
        date_time = review['date'].split('T')
        date = date_time[0].split('-')
        time = date_time[1].split(':')
        the_datetime = datetime(year=int(date[0]), month=int(date[1]), day=int(date[2]),
                                hour=int(time[0]), minute=int(time[1]), second=int(time[2].split('.')[0]))
        contents = f"{review['title']}. {review['body']}"
        contents = ' '.join(get_processed_text(contents, no_stopwords=False))
        user = review['user']
        review_count = review['review_count']
        identifier = f'trustpilot-{user}-{date}-{review_count}'

        self.buffer.append({"id": identifier, "synonyms": {synonym}, "text": contents, "author": user,
                            "date": the_datetime, "num_ratings": review_count})

    def get_buffer_contents(self):
        temp = self.buffer.copy()
        self.buffer.clear()
        return temp
