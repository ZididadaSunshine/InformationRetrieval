from queue import Queue
from threading import Thread
import os
from datetime import datetime
from datetime import timedelta
import time
import traceback
from time import sleep
import requests
import json
from statistics import mean

from dbhandler import DBHandler
from scrapers.trustpilot_crawler import TrustPilotCrawler
from scrapers.reddit_scraper import RedditScraper
from snapshots.snapshot import Snapshot


class Scheduler:

    def __init__(self):
        self.continue_schedule = False

        self.local_db = DBHandler()

        self.all_synonyms = set()

        self.trustpilot = TrustPilotCrawler()
        self.reddit = RedditScraper()

        self.scrapers = {'trustpilot': TrustPilotCrawler(), 'reddit': RedditScraper()}

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
        self.crawler_schedule_thread = Thread()
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
        """
        for scraper in self.scrapers.keys():
            self.scrapers[scraper].begin_crawl()
        """
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
            self.update_synonyms(self.fetch_all_synonyms().keys())

            # Get and commit new posts
            self.commit_reviews(self.retrieve_posts())

            # Get and update sentiments for new posts
            posts = self.fetch_new_posts()
            print(f'{len(posts)} posts fetched')
            if posts:
                sentiments = self.calculate_sentiments(posts)
                try:
                    self.local_db.update_sentiments(sentiments)
                except Exception as e:
                    print(f'Scheduler._threaded_schedule: Exception encountered with local_db.update_sentiments: {e}')
                    traceback.print_exc()
                    # TODO: Handle local_db.update_sentiments exceptions


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
        try:
            predictions = json.loads(requests.post(self.sa_api, json=dict(data=content_list)).text)
        except Exception as e:
            print(f'Scheduler.calculate_sentiments: Exception encountered with SA API: {e}')
            traceback.print_exc()
            # TODO: Handle SA API exceptions
            return []

        # Combine predictions with posts
        results = [{'id': id_list[i], 'sentiment': predictions['predictions'][i]}
                   for i in range(0, len(predictions['predictions']))]

        return results

    def classify_sentiment(self, prediction):
        return prediction >= 0.5

    def retrieve_posts(self):
        # Get posts from each scraper
        r = []
        tp = []
        try:
            r = self.reddit.get_buffer_contents()
        except Exception as e:
            print(f'Scheduler.retrieve_posts: Exception encountered while retrieving posts from reddit: {e}')
            # Initialize and begin a new crawler
            self.reddit = RedditScraper()
            self.reddit.use_synonyms(self.all_synonyms)
            self.reddit.begin_crawl()
        try:
            tp = self.trustpilot.get_buffer_contents()
        except Exception as e:
            print(f'Scheduler.retrieve_posts: Exception encountered while retrieving posts from trustpilot: {e}')
            # Initialize and begin a new crawler
            self.trustpilot = TrustPilotCrawler()
            self.trustpilot.use_synonyms(self.all_synonyms)
            self.trustpilot.begin_crawl()

        """
        for scraper in self.scrapers.items():
            try:
                result[scraper[0]] = self.scrapers[scraper[0]].get_buffer_contents()
            except Exception as e:
                print(f'Scheduler.retrieve_posts: Exception encountered while retrieving posts from {scraper[0]} crawler: {e}')
                traceback.print_exc()
                # TODO: Handle [crawler].get_buffer_contents exceptions
        print(f'scheduler.retrieve_posts: retrieved {result}')
        return result
        """

        result = {'trustpilot': tp,
                'reddit': r}
        print(f'retrieved: {len(result["trustpilot"])} trustpilot posts, {len(result["reddit"])} reddit posts')
        return result

    def commit_reviews(self, reviews):
        # Get reviews from each crawler
        tp_reviews = reviews['trustpilot']
        reddit_reviews = reviews['reddit']

        # Commit reviews to the database
        try:
            for review in tp_reviews:
                self.local_db.commit_trustpilot(identifier=review['id'], synonym=review['synonym'],
                                                contents=review['text'], user=review['author'], date=review['date'],
                                                num_user_ratings=review['num_ratings'])
        except Exception as e:
            print(f'Scheduler.commit_reviews: Exception encountered while commiting trustpilot posts to database: {e}')
            traceback.print_exc()
            # TODO: Handle [db_handler].commit_trustpilot exceptions

        try:
            for review in reddit_reviews:
                self.local_db.commit_reddit(unique_id=review['id'], synonyms=review['synonyms'], text=review['text'],
                                            author=review['author'], date=review['date'], subreddit=review['subreddit'])
        except Exception as e:
            print(f'Scheduler.commit_reviews: Exception encountered while commiting reddit posts to database: {e}')
            traceback.print_exc()
            # TODO: Handle [db_handler].commit_reddit exceptions

    def fetch_all_synonyms(self):
        try:
            synonyms = requests.get(self.synonym_api, headers=self.synonym_api_key).json()
            return synonyms
        except Exception as e:
            print(f'Scheduler.fetch_all_synonyms: Exception encountered with synonym api: {e}')
            return {synonym: -1 for synonym in self.all_synonyms}

    def fetch_new_posts(self, synonym=None, with_sentiment=False):
        """
        Returns all newly crawled posts from the crawler and scraper that
        relate to this synonym.
        :param synonym : string
        :param with_sentiment : boolean - if set to false, only returns rows where sentiment = NULL.
        """
        try:
            posts = self.local_db.get_new_posts(synonym, with_sentiment)
            return posts
        except Exception as e:
            print(f'Scheduler.fetch_new_posts: Exception encountered while retrieving posts from database: {e}')
            traceback.print_exc()
            # TODO: Handle [db_handler].get_new_posts exceptions
            return {}


    def create_snapshot(self, synonym, from_time=datetime.min, to_time=datetime.now()):
        """
        :param synonym: string
        :param from_time: datetime
        :param to_time: datetime
        """
        statistics = dict()
        try:
            posts = self.local_db.get_kwe_posts(synonym, from_time, to_time)
        except Exception as e:
            print(f'Scheduler.create_snapshot: Exception encountered while retrieving posts from database: {e}')
            traceback.print_exc()
            # TODO: Handle [db_handler].get_kwe_posts exception

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
                    try:
                        response = requests.post(self.kwe_api, json=dict(posts=split["posts"]),
                                                 headers=self.kwe_api_key).json()
                        keywords = response.get('keywords', [])
                    except Exception as e:
                        print(f'Scheduler.create_snapshot: Exception encountered with KWE API: {e}')
                        traceback.print_exc()
                        # TODO: Handle KWE API exception

                statistics[split['sentiment_category']] = {"keywords": keywords, "posts": num_posts}
        else:
            return None

        return Snapshot(spans_from=from_time, spans_to=to_time, sentiment=avg_sentiment, synonym=synonym,
                        statistics=statistics)

    def update_synonyms(self, synonyms):
        if set(synonyms) == self.all_synonyms:
            return

        try:
            self.local_db.commit_synonyms(synonyms)
        except Exception as e:
            print(f'Scheduler.update_synonyms: Exception encountered while commiting synonyms to database: {e}')
            traceback.print_exc()
            # TODO: Handle [db_handler].commit_synonyms exceptions
            return

        self.all_synonyms = self.all_synonyms.union(synonyms)
        print("Scheduler.add_synonyms : all_synonyms updated")

        try:
            self.reddit.use_synonyms(self.all_synonyms)
            print("Scheduler.add_synonyms : reddit synonyms updated")
        except Exception as e:
            print(f'Scheduler.update_synonyms: Exception encountered while updating crawler synonyms: {e}')
            # Initialize and begin a new crawler
            self.reddit = RedditScraper()
            self.reddit.use_synonyms(self.all_synonyms)
            self.reddit.begin_crawl()
        try:
            self.trustpilot.use_synonyms(self.all_synonyms)
            print("Scheduler.add_synonyms : trustpilot synonyms updated")
        except Exception as e:
            print(f'Scheduler.update_synonyms: Exception encountered while updating crawler synonyms: {e}')
            # Initialize and begin a new crawler
            self.trustpilot = TrustPilotCrawler()
            self.trustpilot.use_synonyms(self.all_synonyms)
            self.trustpilot.begin_crawl()

    def add_synonym(self, synonym):
        self.add_synonyms([synonym])

    def add_synonyms(self, synonyms):
        self.update_synonyms(list(self.all_synonyms.union(synonyms)))

if __name__ == '__main__':
    s = Scheduler()
    print('Scheduler initialized')
