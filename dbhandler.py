import hashlib

from database import Synonym, Post, SynonymPostAssociation, TrustpilotPost, session_scope
from sqlalchemy.orm import joinedload


class DBHandler:
    def __init__(self):
        pass

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

            # Create synonym if it does not exist
            synonym_inst = self.get_synonym(session, synonym)
            if not synonym_inst:
                synonym_inst = Synonym(name=synonym)
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
                    f'Successfully deleted all data from database: {numSym} synonyms, {numPosts} posts, and {numRels} synonym<->post relations.')

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

    def hash_identifier(self, identifier):
        return hashlib.md5(identifier.encode('utf8')).hexdigest()

    def post_exists(self, session, identifier):
        hashed = self.hash_identifier(identifier)

        return session.query(Post).filter_by(id=hashed).count() >= 1

    def get_synonym(self, session, synonym):
        return session.query(Synonym).filter_by(name=synonym).first()
