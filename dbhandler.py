import datetime
import hashlib

from sqlalchemy.orm import joinedload

from database import Synonym, Post, SynonymPostAssociation, TrustpilotPost, session_scope, RedditPost


class DBHandler:
    def __init__(self):
        pass

    def get_new_posts(self, synonym=None, with_sentiment=False, limit=None):
        with session_scope() as session:
            query = session.query(Post).filter(Post.sentiment.is_(None))
            if limit:
                query = query.order_by(Post.date).limit(limit)

            posts = query.all()

            return {post.id: post.contents for post in posts}

    def get_kwe_posts(self, syn, from_time=datetime.datetime.min , to_time=datetime.datetime.now()):
        with session_scope() as session:

            posts = session.query(Post).\
                filter(Post.sentiment.isnot(None), Post.contents.isnot(None), Post.date >= from_time, Post.date < to_time).\
                join(Post.synonyms).\
                filter(Synonym.name == syn).\
                all()

            return [{"id": post.id, "content": post.contents, "sentiment": post.sentiment} for post in posts]

    def commit_trustpilot(self, synonym, contents, date, identifier, num_user_ratings, user, verbose=False):
        """
        Input:
                synonym          : string
                contents         : string
                date             : UTC datetime object
                identifier       : string
                num_user_ratings : integer
                verbose          : boolean

        Commits a synonym <-> post relation to the database.
        """
        with session_scope() as session:
            post_id = self.hash_identifier(identifier)
            hashed_user = self.hash_identifier(user)

            if self.post_exists(session, identifier):
                return False

            new_post = TrustpilotPost(date=date, contents=contents, id=post_id, user_ratings=num_user_ratings,
                                      author_id=hashed_user)

            # Fetch synonym id from DB
            synonym_inst = self.get_synonym(session, synonym)
            new_post.synonyms = [synonym_inst]

            session.add(new_post)
            session.commit()

            return True

    def clear(self, verbose=False):
        """
        Deletes all synonyms, posts, and relations from the database.
        """
        with session_scope() as session:
            numSym = session.query(Synonym).delete()
            numPosts = session.query(Post).delete()
            numRels = session.query(SynonymPostAssociation).delete()
            session.commit()
            if verbose:
                print(
                    f'''Successfully deleted all data from database: 
                        {numSym} synonyms, 
                        {numPosts} posts, 
                        and {numRels} synonym<->post relations.''')

    def dump(self, clear_data_after=False, verbose=False):
        """
        Dumps all stored data for every synonym to the caller.
        If specified by clear_data_after, clears all tables in the database before returning.
        """
        with session_scope() as session:
            synonyms = session.query(Synonym).options(joinedload('posts')).all()
            if clear_data_after:
                self.clear(verbose=verbose)
            return synonyms

    def set_post_processed(self, session, post, commit=True):
        """ Mark a post as processed, removing its contents at the same time """
        post.contents = None
        post.processed = True

        if commit:
            session.commit()

    def hash_identifier(self, identifier):
        return hashlib.md5(identifier.encode('utf8')).hexdigest()

    def post_exists(self, session, identifier):
        hashed = self.hash_identifier(identifier)

        return session.query(Post).filter_by(id=hashed).count() >= 1

    def get_synonym(self, session, synonym):
        return session.query(Synonym).filter_by(name=synonym).first()

    def commit_synonyms(self, synonyms):
        with session_scope() as session:
            for synonym in synonyms:
                if self.get_synonym(session, synonym):
                    continue

                session.add(Synonym(name=synonym))

            session.commit()

    def update_sentiments(self, sentiments):
        """
        Updates sentiment for posts.
        :param sentiments:
        {
            id        : string,
            sentiment : float
        }
        """
        with session_scope() as session:
            for item in sentiments:
                session.query(Post).filter(Post.id == item["id"]).update({"sentiment": item["sentiment"]})

            session.commit()

    def commit_reddit(self, unique_id, synonyms, text, author, subreddit, date):
        with session_scope() as session:
            if self.post_exists(session, unique_id):
                return False

            synonyms = [self.get_synonym(session, synonym) for synonym in synonyms]
            if None in synonyms:
                raise RuntimeError("Synonyms missing from the database.")

            hashed_author = self.hash_identifier(author)
            hashed_id = self.hash_identifier(unique_id)

            reddit_post = RedditPost(author_id=hashed_author, subreddit=subreddit, synonyms=synonyms, date=date,
                                     contents=text, id=hashed_id)

            session.add(reddit_post)
            session.commit()

            return True
