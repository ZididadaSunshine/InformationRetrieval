import datetime

import praw

from dbhandler import DBHandler
from secrets import Secrets


class RedditSCraper:
    def __init__(self):
        self.db_handler = DBHandler()

        # Initialize reddit client
        self.client = praw.Reddit(client_id=Secrets.REDDIT_CLIENT_ID, client_secret=Secrets.REDDIT_CLIENT_SECRET,
                                  user_agent='Zididada Sunshine')

    def _process_entry(self, entry):
        date = datetime.datetime.utcfromtimestamp(entry.created_utc)
        subreddit = entry.subreddit.display_name


    def scrape_submissions(self):
        for entry in self.client.subreddit('all').stream.submissions():
            if entry.selftext:
                self._process_entry(entry)

    def scrape_comments(self):
        for entry in self.client.subreddit('all').stream.comments():
            self._process_entry(entry)


if __name__ == "__main__":
    RedditSCraper().scrape_submissions()
