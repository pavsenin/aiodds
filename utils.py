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