import unittest
from scrapers.trustpilot_crawler import *


class TrustPilotTestCase(unittest.TestCase):

    def setUp(self):
        self.crawler = TrustPilotCrawler()

    def test_use_synonyms(self):
        synonyms = ['hello', 'google', 'apple', 'andy']
        self.crawler.use_synonyms(synonyms)
        self.assertTrue(len(self.crawler.synonyms) > 0)
        self.assertEqual(len(self.crawler.synonyms), len(self.crawler.synonym_queue.queue))

    def test_can_ping_yet(self):
        self.crawler.host_timer = time.time()
        res, _ = self.crawler.can_ping_yet()
        self.assertFalse(res)

    def test_get_synonym_review_pages(self):
        res = self.crawler._get_synonym_review_pages('apple')
        # Apple always has reviews
        self.assertTrue(len(res) > 0)
        # Must be a full link, not relative
        for review_url in res:
            self.assertTrue('https://www.trustpilot' in review_url)

    def test_is_relevant_review_page(self):
        link_text = 'Google | Adwords'
        res = self.crawler._is_relevant_review_page('google', link_text)
        self.assertTrue(res)

    def test_is_relevant_review_page_negative(self):
        link_text = 'Apple | Adwords'
        res = self.crawler._is_relevant_review_page('google', link_text)
        self.assertFalse(res)

    def test_get_souped_page(self):
        url = 'https://www.trustpilot.com'
        res = self.crawler._get_souped_page(url)
        self.assertTrue(res)
        self.assertTrue(len(res.get_text()) > 0)

    def test_get_reviews_from_url(self):
        url = 'https://www.trustpilot.com/review/www.google.com'
        res, _ = self.crawler._get_reviews_from_url(url)
        self.assertTrue(res)
        for item in res:
            self.assertTrue(type(item['body']) is str)
            self.assertTrue(item['date'] is datetime.date)

    def test_get_next_page(self):
        url = 'https://www.trustpilot.com/review/www.google.com'
        res = self.crawler._get_next_page(self.crawler._get_souped_page(url))
        _, expected = self.crawler._get_reviews_from_url(url)
        self.assertEqual(res, expected)
        self.assertTrue(res)
        self.assertTrue(expected)

    def test_get_date(self):
        soup = self.crawler._get_souped_page('https://www.trustpilot.com/review/www.google.com')
        res = self.crawler._get_date(soup)
        self.assertTrue(type(res) is datetime.date)

    def test_process_entry(self):
        review = {'title': 'the title',
                  'body': 'this is the body',
                  'date': '1994-05-03T3:12:22',
                  'user': 'theuser',
                  'review_count': 123
                  }

        self.crawler._process_entry('synonym', review)
        self.assertTrue(len(self.crawler.buffer) > 0)
        self.assertEqual(self.crawler.buffer[0]['id'], "trustpilot-theuser-['1994', '05', '03']-123")
