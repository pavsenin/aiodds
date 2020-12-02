import gc, time
import pandas as pd
import numpy as np

from datetime import datetime, timedelta
from itertools import product
from utils import Log
from db import DBProvider
from constants import current_seasons, min_participate_matches

class Preprocessor:
    def __init__(self, db, log):
        self.db = db
        self.log = log
    def __select_matches(self, matches_name, leagues_name):
        matches_rows = self.db.select("""SELECT l.season, l.country, l.league_name, m.team_home, m.team_away, m.time, m.score_home, m.score_away, m.was_extra,
m.o1x2_pin_opening_o1_coef, m.o1x2_pin_opening_o0_coef, m.o1x2_pin_opening_o2_coef,
m.o1x2_pin_closing_o1_coef, m.o1x2_pin_closing_o0_coef, m.o1x2_pin_closing_o2_coef, m.o1x2_pin_opening_time,
m.o1x2_mar_opening_o1_coef, m.o1x2_mar_opening_o0_coef, m.o1x2_mar_opening_o2_coef,
m.o1x2_mar_closing_o1_coef, m.o1x2_mar_closing_o0_coef, m.o1x2_mar_closing_o2_coef, m.o1x2_mar_opening_time
FROM """ + matches_name + " m JOIN " + leagues_name + " l ON l.id = m.league_id ORDER BY m.time, l.country, l.league_name"
        )
        if len(matches_rows) == 0:
            return pd.DataFrame()
        return pd.DataFrame(data=matches_rows, columns=list(matches_rows[0].keys()))

    def __extract_features(self, country, league_name, all_data):
        def as_int32(value):
            return 0 if value is None or np.isnan(value) else int(value)
        def get_outcome(row):
            if row['was_extra']:
                return 'o0'
            if row['score_home'] > row['score_away']:
                return 'o1'
            if row['score_home'] < row['score_away']:
                return 'o2'
            return 'o0'
        def get_prob_by_coef(coef):
            if coef == 0.0:
                return 0.0
            return 1 / coef
        def round_to_hour(dt):
            dt_start_of_hour = dt.replace(minute=0, second=0, microsecond=0)
            dt_half_hour = dt.replace(minute=30, second=0, microsecond=0)
            if dt >= dt_half_hour:
                dt = dt_start_of_hour + timedelta(hours=1)
            else:
                dt = dt_start_of_hour
            return dt

        all_data = all_data[(all_data['country'] == country) & (all_data['league_name'] == league_name)]
        all_data.reset_index(drop=True, inplace=True)
        
        all_data['division'] = all_data['country'] + '_' + all_data['league_name']
        all_data['team_home'] = all_data['country'] + '_' + all_data['team_home']
        all_data['team_away'] = all_data['country'] + '_' + all_data['team_away']
        all_data['score_home'] = all_data['score_home'].apply(as_int32)
        all_data['score_away'] = all_data['score_away'].apply(as_int32)
        all_data['real_outcome_o1x2'] = all_data.apply(get_outcome, axis=1)

        teams = list(set(all_data['team_home'].unique()) | set(all_data['team_away'].unique()))
        bad_teams = [team_name for team_name in teams if all_data[(all_data.team_home == team_name) | (all_data.team_away == team_name)].shape[0] < min_participate_matches]
        all_data = all_data[~all_data.team_home.isin(bad_teams)]
        all_data = all_data[~all_data.team_away.isin(bad_teams)]
        teams = [team for team in teams if team not in bad_teams]
        
        # Add results features
        team_results = pd.DataFrame(index = all_data.index, columns=['any_match_team_home_results', 'any_match_team_away_results'])
        for team_name in teams:
            team_data = all_data[(all_data.team_home == team_name) | (all_data.team_away == team_name)]
            team_data = team_data[['team_home', 'team_away', 'score_home', 'score_away', 'real_outcome_o1x2']]
            team_data['team_home_result'] = team_data['real_outcome_o1x2'].map({'o1': 'w', 'o2': 'l', 'o0': 'd'})
            team_data['team_away_result'] = team_data['real_outcome_o1x2'].map({'o1': 'l', 'o2': 'w', 'o0': 'd'})

            def cumcustom(dataframe, map_func):
                data = []
                prev_row_data = None
                for _, row in dataframe.iterrows():
                    row_data = map_func(prev_row_data, row)
                    data.append(row_data)
                    prev_row_data = row_data
                return pd.Series(data, index=dataframe.index)
            def add_team_result(prev_row_data, row):
                result = None
                if row['team_home'] == team_name:
                    result = str(row['team_home_result'])
                elif row['team_away'] == team_name:
                    result = str(row['team_away_result'])
                if prev_row_data is not None:
                    result = result + '_' + prev_row_data
                return result

            team_any_match_results = cumcustom(team_data, add_team_result)
            team_home_index = team_data[team_data['team_home'] == team_name].index
            team_away_index = team_data[team_data['team_away'] == team_name].index
            team_results.loc[team_home_index, 'any_match_team_home_results'] = team_any_match_results
            team_results.loc[team_away_index, 'any_match_team_away_results'] = team_any_match_results

        max_last = 20
        results_columns = []
        team_results_data = pd.DataFrame(index = all_data.index)
        for team in ['team_home', 'team_away']:
            for last in range(1, max_last+1):
                for r in ['w', 'l', 'd']:
                    def get_num_of_results(value):
                        values = value.split('_')[1:last+1]
                        return len([x for x in values if x == r])
                    column = 'any_match_' + team + '_num_' + r + '_last_' + str(last)
                    results_columns.append(column)
                    team_results_data[column] = team_results['any_match_' + team + '_results'].apply(get_num_of_results)
        
        # Add match number features
        match_num_columns = ['any_season_any_match_team_home_match_num', 'any_season_any_match_team_away_match_num', 'current_season_any_match_team_home_match_num', 'current_season_any_match_team_away_match_num']
        match_num_data = pd.DataFrame(index = all_data.index)
        for team_name in teams:
            team_data = all_data[(all_data.team_home == team_name) | (all_data.team_away == team_name)]
            team_data['match_num'] = np.arange(len(team_data))
            team_home_index = team_data[team_data['team_home'] == team_name].index
            team_away_index = team_data[team_data['team_away'] == team_name].index
            match_num_data.loc[team_home_index, 'any_season_any_match_team_home_match_num'] = team_data['match_num']
            match_num_data.loc[team_away_index, 'any_season_any_match_team_away_match_num'] = team_data['match_num']

        for season in all_data['season'].unique():
            season_data = all_data[all_data.season == season]
            for team_name in teams:
                team_data = season_data[(season_data.team_home == team_name) | (season_data.team_away == team_name)]
                team_data['match_num'] = np.arange(len(team_data))
                team_home_index = team_data[team_data['team_home'] == team_name].index
                team_away_index = team_data[team_data['team_away'] == team_name].index
                match_num_data.loc[team_home_index, 'current_season_any_match_team_home_match_num'] = team_data['match_num']
                match_num_data.loc[team_away_index, 'current_season_any_match_team_away_match_num'] = team_data['match_num']
        
        # Add mean features
        mean_columns = [
            'total_score_home_match_team_home_mean', 'total_score_away_match_team_away_mean',
            'o1_home_match_team_home_mean', 'o0_home_match_team_home_mean', 'o2_home_match_team_home_mean',
            'o1_away_match_team_away_mean', 'o0_away_match_team_away_mean', 'o2_away_match_team_away_mean'
        ]

        mean_data = pd.DataFrame(index = all_data.index)
        total_score_home_match_team_home_mean = all_data.groupby('team_home')['score_home'].mean()
        total_score_away_match_team_away_mean = all_data.groupby('team_away')['score_away'].mean()
        outcome_home_match_team_home_mean = all_data.groupby('team_home')['real_outcome_o1x2'].value_counts().unstack(level=-1)
        outcome_home_match_team_home_mean = outcome_home_match_team_home_mean.div(outcome_home_match_team_home_mean.sum(axis=1), axis=0)
        outcome_away_match_team_away_mean = all_data.groupby('team_away')['real_outcome_o1x2'].value_counts().unstack(level=-1)
        outcome_away_match_team_away_mean = outcome_away_match_team_away_mean.div(outcome_away_match_team_away_mean.sum(axis=1), axis=0)

        mean_data['total_score_home_match_team_home_mean'] = all_data['team_home'].map(total_score_home_match_team_home_mean)
        mean_data['total_score_away_match_team_away_mean'] = all_data['team_away'].map(total_score_away_match_team_away_mean)
        mean_data['o1_home_match_team_home_mean'] = all_data['team_home'].map(outcome_home_match_team_home_mean['o1'])
        mean_data['o0_home_match_team_home_mean'] = all_data['team_home'].map(outcome_home_match_team_home_mean['o0'])
        mean_data['o2_home_match_team_home_mean'] = all_data['team_home'].map(outcome_home_match_team_home_mean['o2'])
        mean_data['o1_away_match_team_away_mean'] = all_data['team_away'].map(outcome_away_match_team_away_mean['o1'])
        mean_data['o0_away_match_team_away_mean'] = all_data['team_away'].map(outcome_away_match_team_away_mean['o0'])
        mean_data['o2_away_match_team_away_mean'] = all_data['team_away'].map(outcome_away_match_team_away_mean['o2'])
        
        # Add outcome lags features
        max_lag = 10
        lags = range(1, max_lag + 1)
        lag_names = ['lag_' + str(lag) for lag in lags]
        outcome_lags_columns = ['outcome_any_match_' + ''.join(t) for t in list(product(['team_home_', 'team_away_'], lag_names))]
        outcome_lags_data = pd.DataFrame(index=all_data.index, dtype=np.int32, columns=outcome_lags_columns)
        for team_name in all_data['team_home'].unique():
            def getResult(row):
                if row['team_home_y'] == team_name:
                    if row['real_outcome_o1x2_y'] == 'o1': return 'w'
                    elif row['real_outcome_o1x2_y'] == 'o2': return 'l'
                    else: return 'd'
                else:
                    if row['real_outcome_o1x2_y'] == 'o1': return 'l'
                    elif row['real_outcome_o1x2_y'] == 'o2': return 'w'
                    else: return 'd'

            team_data = all_data[(all_data['team_home'] == team_name) | (all_data['team_away'] == team_name)]
            team_data['match_num'] = np.arange(len(team_data))

            for lag in lags:
                lag_name = 'lag_' + str(lag)

                shifted = team_data[['team_home', 'team_away', 'real_outcome_o1x2', 'match_num']].copy()
                shifted['match_num'] = shifted['match_num'] + lag
                team_data_shifted = team_data.reset_index().merge(shifted, on=['match_num'], how='left').set_index('index')

                team_home_index = team_data[team_data['team_home'] == team_name].index
                team_away_index = team_data[team_data['team_away'] == team_name].index

                team_data_shifted = team_data_shifted[['team_home_x', 'team_away_x', 'real_outcome_o1x2_x', 'team_home_y', 'team_away_y', 'real_outcome_o1x2_y']]

                outcome_any_match_team_home_lag = team_data_shifted.loc[team_home_index].apply(getResult, axis=1)
                outcome_any_match_team_away_lag = team_data_shifted.loc[team_away_index].apply(getResult, axis=1)

                outcome_lags_data.loc[team_home_index, 'outcome_any_match_team_home_' + lag_name] = outcome_any_match_team_home_lag
                outcome_lags_data.loc[team_away_index, 'outcome_any_match_team_away_' + lag_name] = outcome_any_match_team_away_lag
                
        # Add diff_start_time lags features
        max_lag = 10
        lags = range(1, max_lag + 1)
        lag_names = ['lag_' + str(lag) for lag in lags]
        diff_start_time_lags_columns = ['diff_start_time_any_match_' + ''.join(t) for t in list(product(['team_home_', 'team_away_'], lag_names))]
        diff_start_time_lags_data = pd.DataFrame(index=all_data.index, dtype=np.float64, columns=diff_start_time_lags_columns)
        for team_name in teams:
            for lag in lags:
                lag_name = 'lag_' + str(lag)
                team_data = all_data[(all_data['team_home'] == team_name) | (all_data['team_away'] == team_name)]
                diff_start_time_any_match = team_data['time'].diff(lag) / (24 * 60 * 60)
                team_home_index = team_data[team_data['team_home'] == team_name].index
                team_away_index = team_data[team_data['team_away'] == team_name].index
                diff_start_time_lags_data.loc[team_home_index, 'diff_start_time_any_match_team_home_' + lag_name] = diff_start_time_any_match.loc[team_home_index]
                diff_start_time_lags_data.loc[team_away_index, 'diff_start_time_any_match_team_away_' + lag_name] = diff_start_time_any_match.loc[team_away_index]
        
        # Merge all features
        all_data = all_data.join(team_results_data)
        all_data = all_data.join(match_num_data)
        all_data = all_data.join(mean_data)
        all_data = all_data.join(outcome_lags_data)
        all_data = all_data.join(diff_start_time_lags_data)
        
        has_target = all_data[['o1x2_pin_closing_o1_coef', 'o1x2_pin_closing_o0_coef', 'o1x2_pin_closing_o2_coef']].notnull().all(axis=1)
        all_data = all_data[has_target]
        all_data.fillna(0, inplace=True) 
        
        prepared_columns = [
            'country', 'division', 'season', 'team_home', 'team_away',
            'time', 'year', 'month', 'day', 'hour', 'weekday',
        ] + results_columns + match_num_columns + mean_columns + outcome_lags_columns + diff_start_time_lags_columns + [
            'pin_mar_opening_time_diff', 'pin_closing_prob_norm', 
            'pin_opening_prob', 'pin_closing_prob', 'mar_opening_prob', 'mar_closing_prob',
            'pin_opening_coef', 'pin_closing_coef', 'mar_opening_coef', 'mar_closing_coef', 'outcome_o1x2', 'target'
        ]
        
        all_data_prepared = pd.DataFrame(np.repeat(all_data.values, 3, axis=0), columns=all_data.columns)
        time_dt = all_data_prepared['time'].apply(lambda ts: datetime.fromtimestamp(ts))
        all_data_prepared[['year', 'month', 'day', 'hour', 'weekday']] = \
            time_dt.apply(lambda dt: pd.Series([dt.year, dt.month, dt.day, round_to_hour(dt).hour, dt.weekday()]))

        all_data_prepared['outcome_o1x2'] = ['o1', 'o0', 'o2'] * all_data.shape[0]
        all_data_prepared['pin_mar_opening_time_diff'] = all_data_prepared.apply(lambda row: row['o1x2_pin_opening_time'] - row['o1x2_mar_opening_time'], axis=1)
        normalized_probs = 1. / all_data[['o1x2_pin_closing_o1_coef', 'o1x2_pin_closing_o0_coef', 'o1x2_pin_closing_o2_coef']]
        all_data_prepared['pin_closing_prob_norm'] = np.hstack(normalized_probs.div(normalized_probs.sum(axis=1), axis=0).values)
        
        all_data_prepared['pin_opening_coef'] = all_data_prepared.apply(lambda row: row['o1x2_pin_opening_' + row['outcome_o1x2'] + '_coef'], axis=1)
        all_data_prepared['pin_closing_coef'] = all_data_prepared.apply(lambda row: row['o1x2_pin_closing_' + row['outcome_o1x2'] + '_coef'], axis=1)
        all_data_prepared['mar_opening_coef'] = all_data_prepared.apply(lambda row: row['o1x2_mar_opening_' + row['outcome_o1x2'] + '_coef'], axis=1)
        all_data_prepared['mar_closing_coef'] = all_data_prepared.apply(lambda row: row['o1x2_mar_closing_' + row['outcome_o1x2'] + '_coef'], axis=1)
        
        all_data_prepared['pin_opening_prob'] = all_data_prepared['pin_opening_coef'].apply(get_prob_by_coef)
        all_data_prepared['pin_closing_prob'] = all_data_prepared['pin_closing_coef'].apply(get_prob_by_coef)
        all_data_prepared['mar_opening_prob'] = all_data_prepared['mar_opening_coef'].apply(get_prob_by_coef)
        all_data_prepared['mar_closing_prob'] = all_data_prepared['mar_closing_coef'].apply(get_prob_by_coef)
        all_data_prepared['target'] = all_data_prepared.apply(lambda row: 1 if row['real_outcome_o1x2'] == row['outcome_o1x2'] else 0, axis=1)
        all_data_prepared = all_data_prepared[prepared_columns]

        all_data_prepared['time'] = all_data_prepared['time'].astype(np.int32)
        all_data_prepared['year'] = all_data_prepared['year'].astype(np.int32)
        all_data_prepared['month'] = all_data_prepared['month'].astype(np.int32)
        all_data_prepared['day'] = all_data_prepared['day'].astype(np.int32)
        all_data_prepared['hour'] = all_data_prepared['hour'].astype(np.int32)
        all_data_prepared['weekday'] = all_data_prepared['weekday'].astype(np.int32)

        all_data_prepared['pin_mar_opening_time_diff'] = all_data_prepared['pin_mar_opening_time_diff'].astype(np.int32)
        all_data_prepared['pin_closing_prob_norm'] = all_data_prepared['pin_closing_prob_norm'].astype(np.float32)

        all_data_prepared['pin_opening_prob'] = all_data_prepared['pin_opening_prob'].astype(np.float32)
        all_data_prepared['pin_closing_prob'] = all_data_prepared['pin_closing_prob'].astype(np.float32)
        all_data_prepared['mar_opening_prob'] = all_data_prepared['mar_opening_prob'].astype(np.float32)
        all_data_prepared['mar_closing_prob'] = all_data_prepared['mar_closing_prob'].astype(np.float32)

        all_data_prepared['pin_opening_coef'] = all_data_prepared['pin_opening_coef'].astype(np.float32)
        all_data_prepared['pin_closing_coef'] = all_data_prepared['pin_closing_coef'].astype(np.float32)
        all_data_prepared['mar_opening_coef'] = all_data_prepared['mar_opening_coef'].astype(np.float32)
        all_data_prepared['mar_closing_coef'] = all_data_prepared['mar_closing_coef'].astype(np.float32)

        all_data_prepared['outcome_o1x2'] = all_data_prepared['outcome_o1x2'].map({'o1': 1, 'o0': 0, 'o2': 2}).astype(np.int32)

        for results_column in results_columns:
            all_data_prepared[results_column] = all_data_prepared[results_column].astype(np.int32)

        for match_num_column in match_num_columns:
            all_data_prepared[match_num_column] = all_data_prepared[match_num_column].astype(np.int32)

        for mean_column in mean_columns:
            all_data_prepared[mean_column] = all_data_prepared[mean_column].astype(np.float32)

        for outcome_lags_column in outcome_lags_columns:
            all_data_prepared[outcome_lags_column] = all_data_prepared[outcome_lags_column].map({0: 1, 'w': 0, 'd': 1, 'l': 2}).astype(np.int32)

        for diff_start_time_lags_column in diff_start_time_lags_columns:
            all_data_prepared[diff_start_time_lags_column] = all_data_prepared[diff_start_time_lags_column].astype(np.float32)

        all_data_prepared['target'] = all_data_prepared['target'].astype(np.int32)
        
        all_data_prepared = all_data_prepared[all_data_prepared['season'] != '2012']
        all_data_prepared.sort_values(by=['time'], inplace=True)
        all_data_prepared.reset_index(drop=True, inplace=True)
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
        return self.db.execute(executor)

    def preprocess(self):
        pd.options.mode.chained_assignment = None
        archive_matches = self.__select_matches('matches', 'leagues')
        current_matches = self.__select_matches('current_matches', 'current_leagues')
        future_matches = self.__select_matches('future_matches', 'current_leagues')
        all_matches = pd.concat([archive_matches, current_matches, future_matches], ignore_index=True)
        self.log.debug(f"Archive matches {archive_matches.shape[0]}, current matches {current_matches.shape[0]}, future matches {future_matches.shape[0]}, all matches {all_matches.shape[0]}")
        del archive_matches, current_matches, future_matches
        gc.collect()
        self.db.execute(lambda conn: conn.cursor().execute(f"TRUNCATE TABLE preprocessed_matches RESTART IDENTITY;"))
        divisions = [(country, league_name) for _, country, league_name, _ in current_seasons]
        num_matches = 0
        for country, league_name in divisions:
            start_time = time.time()
            extracted = self.__extract_features(country, league_name, all_matches)
            inserted = self.__insert_preprocessed_matches(extracted)
            num_inserted = len(inserted)
            self.log.debug(f"Preprocessed {country} {league_name} Extracted {extracted.shape} Inserted {num_inserted} for {int(time.time() - start_time)} sec")
            assert extracted.shape[0] == num_inserted
            del extracted
            gc.collect()
            num_matches += num_inserted
        return num_matches