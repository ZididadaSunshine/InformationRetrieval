from crawler import TrustPilotCrawler 

cr = TrustPilotCrawler()

print(cr._process_review_page('https://www.trustpilot.com/review/apple-support.it'))