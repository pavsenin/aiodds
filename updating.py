from constants import current_seasons

class BaseUpdater:
    def __init__(self, scraper, match_scraper, db, log):
        self.scraper = scraper
        self.match_scraper = match_scraper
        self.db = db
        self.log = log
    def update_core(self, table_name, get_ids_to_scrap):
        all_inserted_ids = []
        for _, country, league_name, league_id in current_seasons:
            scraped_ids = self.scraper.scrap_match_ids(league_id)
            ids_to_scrap = get_ids_to_scrap(scraped_ids)
            matches = self.scraper.scrap_matches(self.match_scraper, country, league_name, ids_to_scrap)
            inserted_ids = self.db.insert_matches(table_name, league_id, matches)
            all_inserted_ids.extend(inserted_ids)
            self.log.debug(f"Updated {country}\t\t{league_name}\t\tInserted {len(inserted_ids)}")
        self.log.debug(f"Total Inserted {len(all_inserted_ids)}")

class CurrentUpdater(BaseUpdater):
    def __init__(self, scraper, match_scraper, db, log, checker):
        super().__init__(scraper, match_scraper, db, log)
        self.checker = checker
    def __select_match_ids(self, table_name):
        return [row['match_id'] for row in self.db.select(f"SELECT match_id FROM {table_name}")]
    def update(self):
        current_ids = self.__select_match_ids('current_matches')
        future_ids = self.__select_match_ids('future_matches')
        self.update_core('current_matches', lambda scraped_ids: set(scraped_ids) - set(current_ids))
        current_ids_after = self.__select_match_ids('current_matches')
        self.checker.empty_set((set(current_ids) | set(future_ids)) - set(current_ids_after))

class FutureUpdater(BaseUpdater):
    def update(self):
        self.db.execute(lambda conn: conn.cursor().execute(f"DELETE FROM future_matches;"))
        self.update_core('future_matches', lambda scraped_ids: scraped_ids)