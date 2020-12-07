import logging, sys, os
import numpy as np

from math import isclose
from logging import handlers
from datetime import datetime
from urllib.request import Request, urlopen
from bs4 import BeautifulSoup
from abc import ABC, abstractmethod

min_int = -2**31

def get_timestamp():
    return int(datetime.now().timestamp())

def equal(value1, value2):
    if np.isnan(value1) and np.isnan(value2):
        return
    if isclose(value1, value2, rel_tol=1e-5):
        return
    assert value1 == value2

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

def is_invalid(value):
    return value is None or np.isnan(value)
def get_outcome_o1x2(row):
    if row['was_extra']:
        return 'o0'
    if is_invalid(row['score_home']) or is_invalid(row['score_away']):
        return 'o0'
    if row['score_home'] > row['score_away']:
        return 'o1'
    if row['score_home'] < row['score_away']:
        return 'o2'
    return 'o0'

class Log:
    def __init__(self, config, log_name):
        formatter = logging.Formatter("%(message)s")
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        log_file = config.to_abs_path('log', f'{log_name}.log')
        file_handler = handlers.TimedRotatingFileHandler(log_file, when="D", interval=1, backupCount=1)
        file_handler.setFormatter(formatter)
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.DEBUG)
        self.logger.handlers = []
        self.logger.addHandler(console_handler)
        self.logger.addHandler(file_handler)
        self.logger.propagate = False
    def debug(self, message):
        self.logger.debug(message)