import pandas as pd
import time
from datetime import datetime, timedelta

from utils import Log
from db import DBProvider
from preprocessing import Preprocessor
from config_provider import config

db, log = DBProvider(), Log(config, 'debug')
preprocessor = Preprocessor(db, log)
preprocessor.preprocess()