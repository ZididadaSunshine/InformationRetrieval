import time
from datetime import datetime
from threading import Thread
from time import sleep
from urllib.request import urlopen

from bs4 import BeautifulSoup as bs
from retry import retry

from util.orderedsetqueue import OrderedSetQueue, UrlQueue


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
        # The synonym queue is a queue of dictionaries:
        # { synonym : Queue(URL) }
        # When a synonym is popped from the queue, the crawler
        # pops a URL from the synonym's queue. All resulting URLs
        # from the URLs webpage are enqueued in the synonym's URL queue.
        self.synonym_queue = OrderedSetQueue()
        self.buffer = []

        self.host_timer = time.time()
        self.crawled_data = {}
        self.synonyms = set()
        self.seen_reviews = {}
        self.crawler_thread = None

    def begin_crawl(self, synonyms=None, verbose=False):
        if synonyms is not None:
            self.use_synonyms(synonyms, verbose)

        self.crawler_thread = Thread(target=self._threaded_crawl, args=[self.synonym_queue, verbose],
                                     name='Trustpilot Crawler')
        self.crawler_thread.start()

    @retry(delay=0.5, backoff=2, max_delay=60)
    def _threaded_crawl(self, queue, verbose=False):
        while True:
            # Get the next synonym dict in the queue
            url_queue = queue.get()
            if verbose:
                print(f"TrustPilotCrawler._threaded_crawl: {url_queue.tag()} retrieved from synonym_queue")

            # Get the synonym of the URL queue
            synonym = url_queue.tag()

            if url_queue.empty():
                # The queue should be restarted from the initial Trustpilot search.
                self.synonym_queue.put(self._get_url_queue(synonym))
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

            queue.put(url_queue)
            # TODO: Dump data in local database every once in a while.
            # Allow the database to be accessed from anywhere.

    def use_synonyms(self, synonyms, verbose=False):
        if verbose:
            print(f"TrustPilotCrawler.use_synonyms: {len(synonyms)} synonyms retreived")
        new = set(synonyms) - self.synonyms
        url_queues = [self._get_url_queue(synonym) for synonym in new]
        if verbose:
            print(f"TrustPilotCrawler.use_synonyms: {len(url_queues)} url_queues retreived")
            for url_queue in url_queues:
                print(f"TrustPilotCrawler.use_synonyms: {url_queue.tag()}")
        self.synonyms = synonyms
        self._update_synonym_queue(url_queues)

    def add_synonym(self, synonym):
        self.add_synonyms([synonym])

    def add_synonyms(self, synonyms):
        self.use_synonyms(list(self.synonyms.union(synonyms)))

    def _get_url_queue(self, synonym):
        """
        Adds a synonym to the list of tracked synonyms.
        Performs a Trustpilot search for the synonym.
        All query result links are stored in the synonym's queue,
        both of which are added to the synonym queue.
        """
        url_queue = UrlQueue(synonym)
        review_pages = self._get_synonym_review_pages(synonym)

        if len(review_pages) == 0:
            return url_queue

        # Enqueue all pages for this synonym
        for page in review_pages:
            url_queue.put(page)
        return url_queue

    def _update_synonym_queue(self, newurlqueues):
        """
        :param newurlqueues:
        [
            UrlQueue
        ]
        """
        # Add new synonym urlqueues to synonym_queue
        for url_queue in newurlqueues:
            self.synonym_queue.put(url_queue)

        # Filter the unused synonym urlqueues from synonym_queue
        self.synonym_queue.put(None)
        url_queue = self.synonym_queue.get()
        while url_queue is not None:
            if url_queue.tag() in self.synonyms:
                self.synonym_queue.put(url_queue)
            url_queue = self.synonym_queue.get()

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
        return bs(page, features='lxml')

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

        return [{'title': review.find('h2', {'class', 'review-info__body__title'}).get_text().strip(),
                 'body': review.find('p', {'class', 'review-info__body__text'}).get_text().strip(),
                 'date': self._get_date(review),
                 'user': user.strip(),
                 'review_count': review_count
                 } for (review, (user, review_count)) in zip(reviews, users_review_counts)], next_page

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
        user = review['user']
        review_count = review['review_count']
        identifier = f'trustpilot-{user}-{date}-{review_count}'

        self.buffer.append({"id": identifier, "synonym": synonym, "text": body, "author": user,
                            "date": the_datetime, "num_ratings": review_count})

    def get_buffer_contents(self):
        temp = self.buffer.copy()
        self.buffer.clear()
        return temp

    def get_latest_guaranteed_time(self):
        # TODO: return time before which we guarantee all posts have been retrieved.
        pass
