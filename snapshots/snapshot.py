import json
from logging import getLogger

import requests


class Snapshot:
    ISO_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'
    VALID_STATUS_CODES = {}

    def __init__(self, spans_from, spans_to, statistics, sentiment, synonym):
        self.spans_from = spans_from
        self.spans_to = spans_to
        self.statistics = statistics
        self.sentiment = sentiment
        self.synonym = synonym

    def save_remotely(self):
        data = {'from': self.spans_from.strftime(self.ISO_FORMAT),
                'to': self.spans_to.strftime(self.ISO_FORMAT),
                'statistics': json.dumps(self.statistics),
                'sentiment': self.sentiment,
                'synonym': self.synonym}

        try:
            result = requests.post('http://172.28.198.101:8003/api/snapshots', json=data)

            getLogger().info(f'Received status code {result.status_code} while saving snapshot.')
            if result.status_code in self.VALID_STATUS_CODES:
                return True
            else:
                getLogger().error(result.text)
        except Exception as e:
            print(f'Could not establish server contact ({e}).')

        return False
