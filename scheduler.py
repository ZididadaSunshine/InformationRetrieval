from queue import Queue
from threading import Thread
import os
from datetime import datetime
import time
from time import sleep

from dbhandler import DBHandler


class Scheduler:

    def __init__(self):
        # TODO: Get DB location
        #db_url  = os.environ['SENTI_CLOUD_DB_URL']
        #db_user = os.environ['SENTI_CLOUD_USER']
        #db_pw   = os.environ['SENTI_CLOUD_PW']
        #db_name = os.environ['SENTI_CLOUD_DB_NAME']

        # TODO: Set up DB handlers
        self.main_db = None
        self.local_db = DBHandler()

        # TODO: Load synonyms from gateway DB and put them in the queue
        self.synonym_queue = Queue()

        # TODO: Instantiate fields for crawler and scraper
        self.trustpilot = None
        self.reddit = None

        # TODO: Instantiate fields for KWE and SE
        self.kwe = None
        self.se = None

        pass

    def begin_schedule(self):
        # TODO:
        # This should be run as a separate thread.
        # Start a KWE scheduling thread.
        # Repeat the following:
            # Fetch the next synonym from the synonym queue.
            # With this synonym, request from the crawler and
            # scraper all text posts saved with a relation to
            # this synonym.
            # Conduct sentiment analysis for every post immediately.
            # Store text+sentiment+date in a local database.
        # When the KWE scheduler decides it's time (once every hour),
        # fetch all text+sentiment+date from the local database.
        # Conduct KWE for all texts with the same sentiment.
        # In the main database, save the top-5-keywords with a relation
        # to both the synonym and their sentiment.
        pass

    def __threaded_schedule__(self):
        while True:
            next_synonym = self.synonym_queue.get()
            reviews = self.fetch_new_reviews(synonym=next_synonym)
            # TODO: Analyse sentiment for all reviews and store result in local database.

            self.synonym_queue.put(next_synonym)  # Requeue synonym
        pass

    def begin_kwe_schedule(self):
        # TODO:
        # Once every hour, fetch all data from the local database
        # where we have stored text+sentiment+date.
        # Conduct top-5 KWE for texts with the same sentiment.
        # Store the results in the main database.
        pass

    def __threaded_kwe_schedule__(self, wait_for_seconds=3600):
        previous_time = time.mktime(datetime.now().timetuple())
        while True:
            # Wait for scheduled analysis
            next_time = previous_time + wait_for_seconds
            if next_time > previous_time:
                sleep(previous_time - next_time)

            reviews = self.local_db.fetch



        pass

    def fetch_all_synonyms(self):
        # TODO: Fetch all synonyms from the database.
        pass

    def fetch_new_reviews_no_sentiment(self, synonym):
        """
        Returns all newly crawled posts from the crawler and scraper that
        relate to this synonym.
        :param synonym : string
        """
        # TODO: From the crawler and scraper, fetch the new reviews gathered for this synonym.
        return self.local_db.get_new_reviews_no_sentiment(synonym)
        pass

    def commit_keywords_with_sentiment(self, keywords):
        """
        :param keywords:
        {
            date      : UTC datetime object,
            keywords  : string[5],
            sentiment : boolean
        }
        """
        # TODO: Commit keywords w. sentiment to stable storage on main database.
        pass

    def commit_posts_with_sentiment(self, posts):
        """
        Commits text posts (with their assigned sentiment) to local storage.
        :param posts:
        {
            date      : UTC datetime object,
            text      : string,
            sentiment : boolean
        }
        """
        pass

s = Scheduler()
results = s.fetch_new_reviews(synonym='google')
for result in results:
    print(result.contents)