class PostRetriever(): 
    """
    Interface class to be implemented by crawlers, scrapers, and other information
    retrieval classes.
    """

    def __init__(self): 
        pass 

    def fetch_posts(self): 
        """
        Returns the list of posts retrieved since the posts were last fetched. 
        When posts have been fetched from this retriever, they are deleted from 
        temporary storage.  
        """
        pass 

    def search_for(self, synonyms): 
        """
        Begins a crawl/scraping session for a set of synonyms (list of strings). 
        """

    def end_search(self): 
        """
        Ends the current crawl/scraping session."""