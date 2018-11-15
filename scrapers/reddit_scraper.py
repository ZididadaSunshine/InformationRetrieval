import datetime

import praw
from praw.models import Submission, Comment

from dbhandler import DBHandler
from secrets import Secrets
import bs4


class RedditScraper:
    def __init__(self):
        self.db_handler = DBHandler()

        # Initialize reddit client
        self.client = praw.Reddit(client_id=Secrets.REDDIT_CLIENT_ID, client_secret=Secrets.REDDIT_CLIENT_SECRET,
                                  user_agent='Zididada Sunshine')

    def _process_entry(self, entry):
        date = datetime.datetime.utcfromtimestamp(entry.created_utc)
        subreddit = entry.subreddit.display_name
        author = entry.author.name

        body = None
        if isinstance(entry, Submission):
            body = entry.selftext_html
        elif isinstance(entry, Comment):
            body = entry.body_html

        # Remvoe HTML tags from body

        print(date)
        print(subreddit)
        print(author)
        print()

        matching_synonyms = {}

        if not matching_synonyms:
            # If no synonyms match the text, skip it
            return

    def scrape_submissions(self):
        for entry in self.client.subreddit('all').stream.submissions():
            if entry.selftext:
                self._process_entry(entry)

    def scrape_comments(self):
        for entry in self.client.subreddit('all').stream.comments():
            self._process_entry(entry)


if __name__ == "__main__":
    RedditScraper().scrape_comments()
