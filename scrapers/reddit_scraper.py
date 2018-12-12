import datetime
import os
import string
from threading import Thread

import praw
from KeywordExtraction.preprocessing.text_preprocessing import get_processed_text
from bs4 import BeautifulSoup
from praw.models import Submission
from retry import retry


class RedditScraper:
    _remove_table = str.maketrans({key: None for key in string.punctuation})

    def __init__(self):
        self.synonyms = {}
        self.buffer = []
        self.comments_thread = Thread(target=self.scrape_comments, name='Reddit Comment Scraper')
        self.submissions_thread = Thread(target=self.scrape_submissions, name='Reddit Submission Scraper')

        # Initialize reddit client
        self.client = praw.Reddit(client_id=os.environ["REDDIT_CLIENT_ID"],
                                  client_secret=os.environ["REDDIT_CLIENT_SECRET"],
                                  user_agent='Zididada Sunshine')

    def _remove_punctuation(self, text):
        return text.translate(self._remove_table)

    def use_synonyms(self, synonyms):
        self.synonyms = synonyms

    def _normalize(self, token):
        """ Normalize a word by converting it to lowercase and removing punctuation. """
        return self._remove_punctuation(token).lower()

    def _process_entry(self, entry):
        date = datetime.datetime.utcfromtimestamp(entry.created_utc)
        subreddit = entry.subreddit.display_name
        author = entry.author.name
        body = entry.selftext_html if isinstance(entry, Submission) else entry.body_html
        unique_id = str(entry)

        # Remove HTML tags from body
        soup = BeautifulSoup(body, 'lxml')
        body_text = soup.get_text()

        # Split the body into tokens
        tokens = [self._normalize(token) for token in body_text.split()]

        matching_synonyms = set()
        for synonym in self.synonyms:
            if synonym in tokens:
                matching_synonyms.add(synonym)

        # If no synonyms match the text, skip the entry
        if not matching_synonyms:
            return

        # Process text with pre-processor module
        # Stopwords are not removed yet, as they are needed for sentiment analysis
        body_text = ' '.join(get_processed_text(body_text, no_stopwords=False))

        self.buffer.append({'id': unique_id, 'synonyms': matching_synonyms, 'text': body_text, 'author': author,
                            'date': date, 'subreddit': subreddit})

    def get_buffer_contents(self):
        temp = self.buffer.copy()
        self.buffer.clear()
        return temp

    @retry(delay=0.5, backoff=2, max_delay=60)
    def scrape_submissions(self):
        for entry in self.client.subreddit('all').stream.submissions():
            if entry.selftext:
                self._process_entry(entry)

    @retry(delay=0.5, backoff=2, max_delay=60)
    def scrape_comments(self):
        for entry in self.client.subreddit('all').stream.comments():
            self._process_entry(entry)

    def begin_crawl(self):
        self.comments_thread.start()
        self.submissions_thread.start()
