from queue import Queue
from threading import Thread
import os
from datetime import datetime
from datetime import timedelta
import time
from time import sleep
import requests
import json
from statistics import mean

from dbhandler import DBHandler
from scrapers.trustpilot_crawler import TrustPilotCrawler
from scrapers.reddit_scraper import RedditScraper
from snapshots.snapshot import Snapshot


class Scheduler:

    def __init__(self, debug_level):
        self.continue_schedule = False

        # TODO: Set up DB handlers
        self.main_db = None
        self.local_db = DBHandler()

        # TODO: Load synonyms from gateway DB and put them in the queue
        self.synonym_queue = Queue()
        self.all_synonyms = set()

        self.trustpilot = TrustPilotCrawler()
        self.reddit = RedditScraper()

        # TODO: Make Environment Variables for API info
        self.kwe_api = f'http://{os.environ["KWE_API_HOST"]}/'
        self.kwe_api_key = {'Authorization': os.environ['KWE_API_KEY']}
        self.sa_api = f'http://{os.environ["SA_API_HOST"]}/prediction/'
        self.sa_api_key = {'Authorization': os.environ['SA_API_KEY']}
        self.synonym_api = f'http://{os.environ["GATEWAY_API_HOST"]}/api/synonyms'
        self.synonym_api_key = {'Authorization': os.environ['GATEWAY_API_KEY']}

        self.sentiment_categories = [{'category': 'positive', 'upper_limit': 1, 'lower_limit': 0.5},
                                     {'category': 'negative', 'upper_limit': 0.5, 'lower_limit': 0}]

        self.kwe_interval = timedelta(hours=1)
        self.kwe_latest = datetime(2018, 12, 4, 8)

        self.continue_schedule = True
        self.schedule_thread = Thread()
        self.debug = debug_level
        self.begin_schedule()

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
        self.reddit.begin_crawl()
        self.trustpilot.begin_crawl()
        self.continue_schedule = True
        self.schedule_thread = Thread(target=self._threaded_schedule)
        self.schedule_thread.start()

    def _threaded_schedule(self):
        while True:
            if not self.continue_schedule:
                return

            # Retrieve active synonyms from gateway
            self.add_synonyms(self.fetch_all_synonyms().keys())

            # Get and commit new posts
            self.commit_reviews(self.retrieve_posts())

            # Get and update sentiments for new posts
            posts = self.fetch_new_posts()
            if posts:
                sentiments = self.calculate_sentiments(posts)

                self.local_db.update_sentiments(sentiments)

            # Perform keyword extraction and save snapshots from current interval
            if datetime.utcnow() > self.kwe_latest + (2 * self.kwe_interval):
                print(f'Current snapshot date: {self.kwe_latest}')

                for synonym in self.all_synonyms:
                    snapshot = self.create_snapshot(synonym, self.kwe_latest, self.kwe_latest+self.kwe_interval)

                    if snapshot:
                        snapshot.save_remotely()

                self.kwe_latest += self.kwe_interval
            else:
                sleep(10)

    def calculate_sentiments(self, posts):
        """
        :param posts:
        {
            id        : integer,
            text      : string,
        }
        """
        # Extract the post contents
        id_list = []
        content_list = []
        for id, content in posts.items():
            id_list.append(id)
            content_list.append(content)

        # Call the SentimentAnalysis API
        predictions = json.loads(requests.post(self.sa_api, json=dict(data=content_list)).text)

        # Combine predictions with posts
        results = [{'id': id_list[i], 'sentiment': predictions['predictions'][i]}
                   for i in range(0, len(predictions['predictions']))]

        return results

    def classify_sentiment(self, prediction):
        return prediction >= 0.5

    def retrieve_posts(self):
        # Get posts from each scraper
        return {'trustpilot': self.trustpilot.get_buffer_contents(),
                'reddit': self.reddit.get_buffer_contents()}

    def commit_reviews(self, reviews):
        # Get reviews from each crawler
        tp_reviews = reviews['trustpilot']
        reddit_reviews = reviews['reddit']

        # Commit reviews to the database
        for review in tp_reviews:
            self.local_db.commit_trustpilot(identifier=review['id'], synonym=review['synonym'],
                                            contents=review['text'], user=review['author'], date=review['date'],
                                            num_user_ratings=review['num_ratings'])
        for review in reddit_reviews:
            self.local_db.commit_reddit(unique_id=review['id'], synonyms=review['synonyms'], text=review['text'],
                                        author=review['author'], date=review['date'], subreddit=review['subreddit'])

    def fetch_all_synonyms(self):
        return requests.get(self.synonym_api, headers=self.synonym_api_key).json()

    def fetch_new_posts(self, synonym=None, with_sentiment=False):
        """
        Returns all newly crawled posts from the crawler and scraper that
        relate to this synonym.
        :param synonym : string
        :param with_sentiment : boolean - if set to false, only returns rows where sentiment = NULL.
        """
        return self.local_db.get_new_posts(synonym, with_sentiment)

    def create_snapshot(self, synonym, from_time=datetime.min, to_time=datetime.now()):
        """
        :param synonym: string
        :param from_time: datetime
        :param to_time: datetime
        """
        statistics = dict()
        posts = self.local_db.get_kwe_posts(synonym, from_time, to_time)

        if posts:
            avg_sentiment = mean([p["sentiment"] for p in posts])
            splits = [{"sentiment_category": sc["category"],
                       "posts": [p["content"] for p in posts if sc["upper_limit"] >= p["sentiment"] >= sc["lower_limit"]]}
                      for sc in self.sentiment_categories]

            # For each split of posts, compute keywords and number of posts
            for split in splits:
                keywords = []
                num_posts = len(split["posts"])

                # Only requests keywords if there are posts
                if num_posts:
                    response = requests.post(self.kwe_api, json=dict(posts=split["posts"]),
                                             headers=self.kwe_api_key).json()

                    keywords = response.get('keywords', [])

                statistics[split['sentiment_category']] = {"keywords": keywords, "posts": num_posts}
        else:
            return None

        return Snapshot(spans_from=from_time, spans_to=to_time, sentiment=avg_sentiment, synonym=synonym,
                        statistics=statistics)

    def add_synonyms(self, synonyms):
        for synonym in synonyms:
            self.add_synonym(synonym)

    def add_synonym(self, synonym):
        self.local_db.commit_synonyms([synonym])
        self.synonym_queue.put(synonym)
        self.all_synonyms.add(synonym)
        #self.trustpilot.add_synonym(synonym)
        self.reddit.use_synonyms(self.all_synonyms)


if __name__ == '__main__':
    s = Scheduler(3)
    print('Scheduler initialized')
