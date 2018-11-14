import praw
from post_retriever import PostRetriever
from secrets import Secrets


class RedditRetriever(PostRetriever):
    def __init__(self):
        super().__init__()

        # Initialize reddit client
        self.client = praw.Reddit(client_id=Secrets.REDDIT_CLIENT_ID, client_secret=Secrets.REDDIT_CLIENT_SECRET,
                                  user_agent='Zididada Sunshine')

    def _process_post(self, post):
        pass

    def crawl(self):
        for entry in self.client.subreddit('all').stream.comments():
            print(entry.body)


if __name__ == "__main__":
    RedditRetriever().crawl()
