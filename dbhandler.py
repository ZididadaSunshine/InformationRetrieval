import database 
from database import Synonym, Post, SynonymPostAssociation, session
from sqlalchemy.orm import joinedload


class DBHandler(): 

    def __init__(self): 
        pass 

    def commit(self, synonym, post, date, verbose = False):
        """
        Input: 
                Synonym : string
                Post    : string (text body)
                date    : Python datetime object

        Commits a synonym <-> post relation to the database. 
        """ 
        existing_synonyms = [synonym.name for synonym in session.query(Synonym)]
        synonym_exists = synonym in existing_synonyms

        oPost = Post(date = date, contents = post)

        # Check if synonym exists 
        if synonym_exists: 
            oSyn = session.query(Synonym).filter_by(name = synonym).first()
            oSyn.posts.append(oPost)
            if verbose: 
                print(f'Adding post to existing synonym in database ({synonym}).')

        else: 
            oSyn = Synonym(name = synonym)
            oSyn.posts.append(oPost)
            session.add(oSyn)
            if verbose: 
                print(f'Adding {synonym} to database.')

        session.commit()

    def clear(self, verbose = False):
        """
        Deletes all synonyms, posts, and relations from the database.
        """
        numSym = session.query(Synonym).delete()
        numPosts = session.query(Post).delete()
        numRels = session.query(SynonymPostAssociation).delete()
        session.commit()
        if verbose: 
            print(f'Successfully deleted all data from database: {numSym} synonyms, {numPosts} posts, and {numRels} synonym<->post relations.')

    def dump(self, clear_data_after = False, verbose = False): 
        """
        Dumps all stored data for every synonym to the caller. 
        If specified by clear_data_after, clears all tables in the database before returning.
        """
        synonyms = session.query(Synonym).options(joinedload('posts')).all()
        if clear_data_after: 
            self.clear(verbose = verbose)
        return synonyms


