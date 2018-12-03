from queue import Queue
from threading import Thread
import os
from datetime import datetime
import time
from time import sleep
import requests
import json

from dbhandler import DBHandler
from scrapers.trustpilot_crawler import TrustPilotCrawler
from scrapers.reddit_scraper import RedditScraper


class Scheduler:

    def __init__(self):

        # TODO: Set up DB handlers
        self.main_db = None
        self.local_db = DBHandler()

        # TODO: Load synonyms from gateway DB and put them in the queue
        self.synonym_queue = Queue()
        self.all_synonyms = set()

        # TODO: Instantiate fields for crawler and scraper
        self.trustpilot = TrustPilotCrawler()
        self.reddit = RedditScraper()

        # TODO: Instantiate fields for KWE and SE
        self.kwe = None
        self.se = "http://172.28.198.101:8002/prediction/"

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

    def _threaded_schedule(self):
        while True:
            # get synonyms
            synonyms = self.fetch_all_synonyms()
            synonym_keys = synonyms.keys()
            self.add_synonyms(synonym_keys)

            # get new reviews
            self.retrieve_and_commit_reviews()

            # TODO: Analyse sentiment for all reviews and store result in local database.
            for synonym in synonym_keys:
                reviews = self.fetch_new_reviews(synonym, with_sentiment=False)
                #posts_with_sentiment = self.calculate_sentiment()
                #self.commit_posts_with_sentiment(posts_with_sentiment)

            sleep(2)
        pass

    def calculate_sentiment(self, posts):
        """
        :param posts:
        {
            id        : integer,
            text      : string,
            sentiment : integer
        }
        """
        # Extract the post contents
        text = []
        for item in posts:
            text.append(item["text"])
        print(text)

        # Call the SentimentAnalysis API
        predictions = json.loads(requests.post(self.se, json=dict(data=text)).text)
        print(predictions)

        # Assign the predictions to the posts
        for i in range(0, len(predictions["predictions"])):
            posts[i]["sentiment"] = predictions["predictions"][i]

        return posts

    def classify_sentiment(self, prediction):
        return prediction >= 0.5

    def commit_sentiments(self, sentiments):
        # TODO: store sentiments in local database.
        pass

    def retrieve_and_commit_reviews(self):
        # Get reviews from each crawler
        tp_reviews = self.trustpilot.get_buffer_contents()
        reddit_reviews = self.reddit.get_buffer_contents()

        # Commit reviews to the database
        for review in tp_reviews:
            self.local_db.commit_trustpilot(identifier=review["id"], synonym=review["synonym"],
                                            contents=review["text"], user=review["author"], date=review["date"],
                                            num_user_ratings=review["num_ratings"])
        for review in reddit_reviews:
            self.local_db.commit_reddit(unique_id=review["id"], synonyms=review["synonyms"], text=review["text"],
                                        author=review["author"], date=review["date"], subreddit=review["subreddit"])


    def begin_kwe_schedule(self):
        # TODO:
        # Once every hour, fetch all data from the local database
        # where we have stored text+sentiment+date.
        # Conduct top-5 KWE for texts with the same sentiment.
        # Store the results in the main database.
        pass

    def _threaded_kwe_schedule(self, wait_for_seconds=3600):
        previous_time = time.mktime(datetime.now().timetuple())
        while True:
            # Wait for scheduled analysis
            next_time = previous_time + wait_for_seconds
            if next_time > previous_time:
                sleep(previous_time - next_time)

            # Make a list of all synonyms.
            # For each of them, fetch all their new posts with sentiment.
            # Get keywords and construct the return value: {synonym : {'good' : [], 'bad' : []}}
            for synonym in self.all_synonyms:
                # Sentiment analysis, store results in gateway DB
                pass

        pass

    def fetch_all_synonyms(self):
        return json.loads(requests.get("http://172.28.198.101:8000/api/synonyms").json())

    def fetch_new_reviews(self, synonym, with_sentiment=False):
        """
        Returns all newly crawled posts from the crawler and scraper that
        relate to this synonym.
        :param synonym : string
        :param with_sentiment : boolean - if set to false, only returns rows where sentiment = NULL.
        """
        return self.local_db.get_new_reviews(synonym, with_sentiment=with_sentiment)
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

    def add_synonyms(self, synonyms):
        for synonym in synonyms:
            self.add_synonym(synonym)

    def add_synonym(self, synonym):
        self.local_db.commit_synonyms([synonym])
        self.synonym_queue.put(synonym)
        self.all_synonyms.add(synonym)
        self.trustpilot.add_synonym(synonym)
        self.reddit.use_synonyms(self.all_synonyms)


s = Scheduler()
#s.add_synonyms(['dsb', 'apple', 'google'])
#s.trustpilot.begin_crawl()

#sleep(10)

#results = s.fetch_new_reviews(synonym='google', with_sentiment=False)
#print(len(results))
#for result in results:
#    print(result.contents)

saresults = s.calculate_sentiment([{"id": 1, "text": "jarlun is a great product that is very good", "sentiment": False},
                                  {"id": 2, "text": "jarlund is a bad product that sucks", "sentiment": False}])

print(len(saresults))
for result in saresults:
    print(result)