from constants import current_seasons

class BaseUpdater:
    def __init__(self, scraper, match_scraper, db, log):
        self.scraper = scraper
        self.match_scraper = match_scraper
        self.db = db
        self.log = log
    def __insert_matches(self, table_name, league_id, matches):
        int_league_id = self.db.select(f"SELECT id FROM current_leagues WHERE league_id = '{league_id}'")
        int_league_id = int_league_id[0]['id']
        inserted_matches = []
        for match_info in matches:
            columns = ', '.join(match_info.keys())
            values = self.db.join_values(match_info.values())
            def insert_executor(conn):
                cursor = conn.cursor()
                insert_match_request = f"INSERT INTO {table_name} (league_id, {columns}) VALUES ({int_league_id}, {values}) RETURNING match_id;"
                cursor.execute(insert_match_request)
                return cursor.fetchone()[0]
            inserted = self.db.execute(insert_executor)
            inserted_matches.append(inserted)
        return inserted_matches
    def update_core(self, table_name, get_ids_to_scrap):
        all_inserted_ids = []
        for _, country, league_name, league_id in current_seasons:
            scraped_ids = self.scraper.scrap_match_ids(league_id)
            ids_to_scrap = get_ids_to_scrap(scraped_ids)
            matches = self.scraper.scrap_matches(self.match_scraper, country, league_name, ids_to_scrap)
            inserted_ids = self.__insert_matches(table_name, league_id, matches)
            all_inserted_ids.extend(inserted_ids)
            self.log.debug(f"Updated {country} {league_name} Inserted {len(inserted_ids)}")
        self.log.debug(f"Total Inserted {len(all_inserted_ids)}")

class CurrentUpdater(BaseUpdater):
    def __select_match_ids(self, table_name):
        return [row['match_id'] for row in self.db.select(f"SELECT match_id FROM {table_name}")]
    def __debug_urls_from_ids(self, title_log, table_name, ids):
        if len(list(ids)) == 0:
            return
        rows = self.db.select(f"SELECT CONCAT('https://www.oddsportal.com/soccer/', l.country, '/', l.league_name, '/', m.match_id, '/') AS url \
            FROM {table_name} m JOIN current_leagues l ON l.id = m.league_id WHERE match_id IN ({self.db.join_values(ids)})")
        if len(list(rows)) == 0:
            return
        self.log.debug(f"{title_log} {len(ids)}")
        for row in rows: self.log.debug(row['url'])
    def update(self):
        current_ids = self.__select_match_ids('current_matches')
        future_ids = self.__select_match_ids('future_matches')
        self.update_core('current_matches', lambda scraped_ids: set(scraped_ids) - set(current_ids))
        current_ids_after = self.__select_match_ids('current_matches')
        self.__debug_urls_from_ids('Non-handled future', 'future_matches', (set(current_ids) | set(future_ids)) - set(current_ids_after))
        self.__debug_urls_from_ids('Unexpected current', 'current_matches', set(current_ids_after) - (set(current_ids) | set(future_ids)))

class FutureUpdater(BaseUpdater):
    def update(self):
        self.db.execute(lambda conn: conn.cursor().execute(f"TRUNCATE TABLE future_matches RESTART IDENTITY;"))
        self.update_core('future_matches', lambda scraped_ids: scraped_ids)