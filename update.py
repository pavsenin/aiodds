import time
from datetime import datetime, timedelta
from updating import CurrentUpdater, FutureUpdater

from utils import Log
from db import DBProvider
from scraping import MatchScraper, LeagueScraper, FutureLeagueScraper

now = datetime.now()
db, log = DBProvider(), Log()

now_morning = datetime(now.year, now.month, now.day, 4)
start_time = time.time()
log.debug(f"Update current_matches to {now_morning}")
updater = CurrentUpdater(LeagueScraper(), MatchScraper(from_time=None, to_time=now_morning), db, log)
updater.update()
log.debug(f"Updated current_matches for {int(time.time() - start_time)} sec")

next_day = now + timedelta(days=1)
next_day_morning = datetime(next_day.year, next_day.month, next_day.day, 4)
start_time = time.time()
log.debug(f"Update future_matches from {now_morning} to {next_day_morning}")
updater = FutureUpdater(FutureLeagueScraper(), MatchScraper(from_time=now_morning, to_time=next_day_morning), db, log)
updater.update()
log.debug(f"Updated future_matches for {int(time.time() - start_time)} sec")