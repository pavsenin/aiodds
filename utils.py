import logging, sys

from inspect import stack
from datetime import datetime
from urllib.request import Request, urlopen
from bs4 import BeautifulSoup

def get_timestamp():
    return int(datetime.now().timestamp())

def get_timestamp_for_url(multiplier):
    return str(get_timestamp()*multiplier)

def fetch_url(url, headers=None):
    request = Request(url)
    if headers:
        for k, v in headers.items():
            request.add_header(k, v)
    stream = urlopen(request)
    content = stream.read().decode('utf8')
    stream.close()
    return content

def fetch_url_oddsportal(url):
    headers = { 'Host': 'fb.oddsportal.com', 'Referer': 'www.oddsportal.com' }
    return fetch_url(url, headers)

def retry(action, args, max_retry):
    value, i = None, 1
    while value is None and i < max_retry:
        value = action(args)
        i += 1
    return value

def parse_tree(content):
    return BeautifulSoup(content, 'lxml')

class Log:
    def __init__(self):
        formatter = logging.Formatter("%(asctime)s %(levelname)s: %(message)s")
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.DEBUG)
        self.logger.handlers = []
        self.logger.addHandler(console_handler)
        self.logger.propagate = False
    def debug(self, message):
        self.logger.debug(message)
class Assert:
    def equal(self, value1, value2):
        assert value1 == value2
    def empty_set(self, set1):
        assert not bool(set1), f"{set1}"