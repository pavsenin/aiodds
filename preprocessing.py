import gc, time
import pandas as pd
import numpy as np

from datetime import datetime, timedelta
from itertools import product
from utils import Log, get_outcome_o1x2
from db import DBProvider
from feature_extracting import TimeExtractor, MeanExtractor, LastResultsExtractor, MatchNumExtractor,  OutcomeLagsExtractor, TimeLagsExtractor
from constants import current_seasons, bookmakers_map, min_participate_matches, pin_name

class Preprocessor:
    def __init__(self, db, log):
        self.db = db
        self.log = log
#     def __select_preprocessed_md5(self):
#         request = """SELECT md5(fast_hashagg(md5(CAST(pm.* AS text)))) AS hash FROM (
# 	SELECT country, division, season, team_home, team_away, time, year, month, day, hour, weekday, any_match_team_home_num_w_last_1, any_match_team_home_num_l_last_1, any_match_team_home_num_d_last_1, any_match_team_home_num_w_last_2, any_match_team_home_num_l_last_2, any_match_team_home_num_d_last_2, any_match_team_home_num_w_last_3, any_match_team_home_num_l_last_3, any_match_team_home_num_d_last_3, any_match_team_home_num_w_last_4, any_match_team_home_num_l_last_4, any_match_team_home_num_d_last_4, any_match_team_home_num_w_last_5, any_match_team_home_num_l_last_5, any_match_team_home_num_d_last_5, any_match_team_home_num_w_last_6, any_match_team_home_num_l_last_6, any_match_team_home_num_d_last_6, any_match_team_home_num_w_last_7, any_match_team_home_num_l_last_7, any_match_team_home_num_d_last_7, any_match_team_home_num_w_last_8, any_match_team_home_num_l_last_8, any_match_team_home_num_d_last_8, any_match_team_home_num_w_last_9, any_match_team_home_num_l_last_9, any_match_team_home_num_d_last_9, any_match_team_home_num_w_last_10, any_match_team_home_num_l_last_10, any_match_team_home_num_d_last_10, any_match_team_home_num_w_last_11, any_match_team_home_num_l_last_11, any_match_team_home_num_d_last_11, any_match_team_home_num_w_last_12, any_match_team_home_num_l_last_12, any_match_team_home_num_d_last_12, any_match_team_home_num_w_last_13, any_match_team_home_num_l_last_13, any_match_team_home_num_d_last_13, any_match_team_home_num_w_last_14, any_match_team_home_num_l_last_14, any_match_team_home_num_d_last_14, any_match_team_home_num_w_last_15, any_match_team_home_num_l_last_15, any_match_team_home_num_d_last_15, any_match_team_home_num_w_last_16, any_match_team_home_num_l_last_16, any_match_team_home_num_d_last_16, any_match_team_home_num_w_last_17, any_match_team_home_num_l_last_17, any_match_team_home_num_d_last_17, any_match_team_home_num_w_last_18, any_match_team_home_num_l_last_18, any_match_team_home_num_d_last_18, any_match_team_home_num_w_last_19, any_match_team_home_num_l_last_19, any_match_team_home_num_d_last_19, any_match_team_home_num_w_last_20, any_match_team_home_num_l_last_20, any_match_team_home_num_d_last_20, any_match_team_away_num_w_last_1, any_match_team_away_num_l_last_1, any_match_team_away_num_d_last_1, any_match_team_away_num_w_last_2, any_match_team_away_num_l_last_2, any_match_team_away_num_d_last_2, any_match_team_away_num_w_last_3, any_match_team_away_num_l_last_3, any_match_team_away_num_d_last_3, any_match_team_away_num_w_last_4, any_match_team_away_num_l_last_4, any_match_team_away_num_d_last_4, any_match_team_away_num_w_last_5, any_match_team_away_num_l_last_5, any_match_team_away_num_d_last_5, any_match_team_away_num_w_last_6, any_match_team_away_num_l_last_6, any_match_team_away_num_d_last_6, any_match_team_away_num_w_last_7, any_match_team_away_num_l_last_7, any_match_team_away_num_d_last_7, any_match_team_away_num_w_last_8, any_match_team_away_num_l_last_8, any_match_team_away_num_d_last_8, any_match_team_away_num_w_last_9, any_match_team_away_num_l_last_9, any_match_team_away_num_d_last_9, any_match_team_away_num_w_last_10, any_match_team_away_num_l_last_10, any_match_team_away_num_d_last_10, any_match_team_away_num_w_last_11, any_match_team_away_num_l_last_11, any_match_team_away_num_d_last_11, any_match_team_away_num_w_last_12, any_match_team_away_num_l_last_12, any_match_team_away_num_d_last_12, any_match_team_away_num_w_last_13, any_match_team_away_num_l_last_13, any_match_team_away_num_d_last_13, any_match_team_away_num_w_last_14, any_match_team_away_num_l_last_14, any_match_team_away_num_d_last_14, any_match_team_away_num_w_last_15, any_match_team_away_num_l_last_15, any_match_team_away_num_d_last_15, any_match_team_away_num_w_last_16, any_match_team_away_num_l_last_16, any_match_team_away_num_d_last_16, any_match_team_away_num_w_last_17, any_match_team_away_num_l_last_17, any_match_team_away_num_d_last_17, any_match_team_away_num_w_last_18, any_match_team_away_num_l_last_18, any_match_team_away_num_d_last_18, any_match_team_away_num_w_last_19, any_match_team_away_num_l_last_19, any_match_team_away_num_d_last_19, any_match_team_away_num_w_last_20, any_match_team_away_num_l_last_20, any_match_team_away_num_d_last_20, any_season_any_match_team_home_match_num, any_season_any_match_team_away_match_num, current_season_any_match_team_home_match_num, current_season_any_match_team_away_match_num, total_score_home_match_team_home_mean, total_score_away_match_team_away_mean, o1_home_match_team_home_mean, o0_home_match_team_home_mean, o2_home_match_team_home_mean, o1_away_match_team_away_mean, o0_away_match_team_away_mean, o2_away_match_team_away_mean, outcome_any_match_team_home_lag_1, outcome_any_match_team_home_lag_2, outcome_any_match_team_home_lag_3, outcome_any_match_team_home_lag_4, outcome_any_match_team_home_lag_5, outcome_any_match_team_home_lag_6, outcome_any_match_team_home_lag_7, outcome_any_match_team_home_lag_8, outcome_any_match_team_home_lag_9, outcome_any_match_team_home_lag_10, outcome_any_match_team_away_lag_1, outcome_any_match_team_away_lag_2, outcome_any_match_team_away_lag_3, outcome_any_match_team_away_lag_4, outcome_any_match_team_away_lag_5, outcome_any_match_team_away_lag_6, outcome_any_match_team_away_lag_7, outcome_any_match_team_away_lag_8, outcome_any_match_team_away_lag_9, outcome_any_match_team_away_lag_10, diff_start_time_any_match_team_home_lag_1, diff_start_time_any_match_team_home_lag_2, diff_start_time_any_match_team_home_lag_3, diff_start_time_any_match_team_home_lag_4, diff_start_time_any_match_team_home_lag_5, diff_start_time_any_match_team_home_lag_6, diff_start_time_any_match_team_home_lag_7, diff_start_time_any_match_team_home_lag_8, diff_start_time_any_match_team_home_lag_9, diff_start_time_any_match_team_home_lag_10, diff_start_time_any_match_team_away_lag_1, diff_start_time_any_match_team_away_lag_2, diff_start_time_any_match_team_away_lag_3, diff_start_time_any_match_team_away_lag_4, diff_start_time_any_match_team_away_lag_5, diff_start_time_any_match_team_away_lag_6, diff_start_time_any_match_team_away_lag_7, diff_start_time_any_match_team_away_lag_8, diff_start_time_any_match_team_away_lag_9, diff_start_time_any_match_team_away_lag_10, pin_mar_opening_time_diff, pin_closing_prob_norm, pin_opening_prob, pin_closing_prob, mar_opening_prob, mar_closing_prob, pin_opening_coef, pin_closing_coef, mar_opening_coef, mar_closing_coef, outcome_o1x2, target FROM preprocessed_matches
# 	WHERE TO_TIMESTAMP(time) < CURRENT_DATE::timestamp - INTERVAL '20 hour'
# 	ORDER BY time, division, team_home, team_away, outcome_o1x2
# ) pm"""
#         return self.db.select(request)[0]['hash']
    def __select_matches(self, matches_name, leagues_name):
        matches_rows = self.db.select("""SELECT l.season, l.country, l.league_name, m.* FROM """ + matches_name +
            " m JOIN " + leagues_name + " l ON l.id = m.league_id ORDER BY m.time, l.country, l.league_name"
        )
        if len(matches_rows) == 0:
            return pd.DataFrame()
        return pd.DataFrame(data=matches_rows, columns=list(matches_rows[0].keys()))

    def __get_teams(self, all_data):
        teams = list(set(all_data['team_home'].unique()) | set(all_data['team_away'].unique()))
        bad_teams = [team_name for team_name in teams if all_data[(all_data.team_home == team_name) | (all_data.team_away == team_name)].shape[0] < min_participate_matches]
        all_data = all_data[~all_data.team_home.isin(bad_teams)]
        all_data = all_data[~all_data.team_away.isin(bad_teams)]
        teams = [team for team in teams if team not in bad_teams]
        return teams

    def __get_league_matches(self, country, league_name, all_matches):
        league_matches = all_matches[(all_matches['country'] == country) & (all_matches['league_name'] == league_name)]
        league_matches['real_outcome_o1x2'] = league_matches.apply(get_outcome_o1x2, axis=1)
        league_matches.sort_values(by=['time', 'country', 'league_name', 'team_home', 'team_away'], inplace=True)
        league_matches.reset_index(drop=True, inplace=True)
        return league_matches

    def __extract_features(self, all_data):
        all_prepared_columns = ['country', 'division', 'season', 'team_home', 'team_away']
        all_data['division'] = all_data['country'] + '_' + all_data['league_name']
        all_data['team_home'] = all_data['country'] + '_' + all_data['team_home']
        all_data['team_away'] = all_data['country'] + '_' + all_data['team_away']
        all_data['time'] = all_data['time'].astype(np.int32)
        teams = self.__get_teams(all_data)

        extractors = [TimeExtractor()]# , MeanExtractor(), LastResultsExtractor(teams), MatchNumExtractor(teams),  OutcomeLagsExtractor(teams), TimeLagsExtractor(teams)]
        for extractor in extractors:
            extracted_columns, extracted_data = extractor.extract(all_data)
            all_data = all_data.join(extracted_data)
            all_prepared_columns += extracted_columns
        
        target_o1x2_columns = ['o1x2_pin_opening_o1_coef', 'o1x2_pin_opening_o0_coef', 'o1x2_pin_opening_o2_coef', 'o1x2_pin_closing_o1_coef', 'o1x2_pin_closing_o0_coef', 'o1x2_pin_closing_o2_coef']
        has_target = all_data[target_o1x2_columns].notnull().all(axis=1)
        all_data = all_data[has_target]
        
        all_data_prepared = pd.DataFrame(np.repeat(all_data.values, 3, axis=0), columns=all_data.columns)
        all_data_prepared['outcome_o1x2'] = ['o1', 'o0', 'o2'] * all_data.shape[0]

        # for book_name in bookmakers_map.values():
        #     feature_extractor = PinExtractor(all_data) if book_name == 'pin' else BookExtractor(book_name, all_data)
        #     columns, all_data_prepared = feature_extractor.extract(all_data_prepared)
        #     prepared_columns += columns

        all_data_prepared['outcome_o1x2'] = all_data_prepared['outcome_o1x2'].map({'o1': 1, 'o0': 0, 'o2': 2}).astype(np.int32)
        all_data_prepared['target_o1x2'] = all_data_prepared.apply(lambda row: 1 if row['real_outcome_o1x2'] == row['outcome_o1x2'] else 0, axis=1).astype(np.int32)
        all_prepared_columns += ['outcome_o1x2', 'target_o1x2']

        all_data_prepared = all_data_prepared[all_prepared_columns]
        all_data_prepared = all_data_prepared[all_data_prepared['season'] != '2012']
        return all_data_prepared

    def __insert_preprocessed_matches(self, matches):
        def executor(conn):
            columns = ', '.join([col for col in matches.columns.values])
            insert_values = [self.db.join_values(row.values) for _, row in matches.iterrows()]
            values_string = ', '.join([f'({values})' for values in insert_values])
            insert_match_request = f"INSERT INTO preprocessed_matches ({columns}) VALUES {values_string} RETURNING id;"
            cursor = conn.cursor()
            cursor.execute(insert_match_request)
            return cursor.fetchall()
        inserted = self.db.execute(executor)
        return len(inserted)

    def __save_preprocessed_matches(self, country, league_name, all_data):
        file_path = f'./data/{country}_{league_name}.csv'
        all_data.to_csv(file_path)
        return all_data.shape[0]

    def preprocess(self):
        pd.options.mode.chained_assignment = None
        # before_hash = self.__select_preprocessed_md5()
        # self.log.debug(f"Before preprocessing matches md5 is {before_hash}")
        archive_matches = self.__select_matches('matches', 'leagues')
        current_matches = self.__select_matches('current_matches', 'current_leagues')
        future_matches = self.__select_matches('future_matches', 'current_leagues')
        all_matches = pd.concat([archive_matches, current_matches, future_matches], ignore_index=True)
        self.log.debug(f"Archive matches {archive_matches.shape[0]}, current matches {current_matches.shape[0]}, future matches {future_matches.shape[0]}, all matches {all_matches.shape[0]}")
        del archive_matches, current_matches, future_matches; gc.collect()
        self.db.execute(lambda conn: conn.cursor().execute(f"TRUNCATE TABLE preprocessed_matches RESTART IDENTITY;"))
        divisions = [(country, league_name) for _, country, league_name, _ in current_seasons]
        num_matches = 0
        for country, league_name in divisions:
            start_time = time.time()
            league_matches = self.__get_league_matches(country, league_name, all_matches)
            extracted = self.__extract_features(league_matches)
            num_inserted = self.__save_preprocessed_matches(country, league_name, extracted)
            # num_inserted = self.__insert_preprocessed_matches(extracted)
            self.log.debug(f"Preprocessed {country} {league_name} Extracted {extracted.shape} Inserted {num_inserted} for {int(time.time() - start_time)} sec")
            assert extracted.shape[0] == num_inserted
            del extracted; gc.collect()
            num_matches += num_inserted
        # after_hash = self.__select_preprocessed_md5()
        # self.log.debug(f"After preprocessing matches md5 is {after_hash}")
        #assert before_hash == after_hash
        return num_matches