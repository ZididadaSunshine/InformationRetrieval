import datetime
import re
import string

import praw
from bs4 import BeautifulSoup
from praw.models import Submission

from dbhandler import DBHandler
from secrets import Secrets


class RedditScraper:
    _remove_table = str.maketrans({key: None for key in string.punctuation})

    def __init__(self):
        self.db_handler = DBHandler()
        self.synonyms = {}

        # Initialize reddit client
        self.client = praw.Reddit(client_id=Secrets.REDDIT_CLIENT_ID, client_secret=Secrets.REDDIT_CLIENT_SECRET,
                                  user_agent='Zididada Sunshine')

    def _remove_punctuation(self, text):
        return text.translate(self._remove_table)

    def use_synonyms(self, synonyms):
        self.synonyms = synonyms
        self.db_handler.commit_synonyms(synonyms)

    def _normalize(self, token):
        """ Normalize a word by converting it to lowercase and removing puncutation """
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

        self.db_handler.commit_reddit(unique_id, matching_synonyms, body_text, author, subreddit, date)

    def scrape_submissions(self):
        for entry in self.client.subreddit('all').stream.submissions():
            if entry.selftext:
                self._process_entry(entry)

    def scrape_comments(self):
        for entry in self.client.subreddit('all').stream.comments():
            self._process_entry(entry)


if __name__ == "__main__":
    scraper = RedditScraper()
    scraper.use_synonyms({'apple', 'elon', 'ea', 'amazon', 'denmark', 'sweden', 'trump'})
    scraper.scrape_comments()
