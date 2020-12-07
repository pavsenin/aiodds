import numpy as np
import pandas as pd

from datetime import datetime, timedelta
from itertools import product
from abc import ABC, abstractmethod
from utils import min_int

class BaseExtractor(ABC):
    @abstractmethod
    def extract(self, all_data):
        pass

class TimeExtractor(BaseExtractor):
    def __round_to_hour(self, dt):
        dt_start_of_hour = dt.replace(minute=0, second=0, microsecond=0)
        dt_half_hour = dt.replace(minute=30, second=0, microsecond=0)
        if dt >= dt_half_hour:
            dt = dt_start_of_hour + timedelta(hours=1)
        else:
            dt = dt_start_of_hour
        return dt
    def extract(self, all_data):
        time_columns = ['year', 'month', 'day', 'hour', 'weekday']
        time_data = pd.DataFrame(index = all_data.index)
        time_dt = all_data['time'].apply(lambda ts: datetime.fromtimestamp(ts))
        time_data[time_columns] = time_dt.apply(lambda dt: pd.Series([dt.year, dt.month, dt.day, self.__round_to_hour(dt).hour, dt.weekday()]))
        for time_column in time_columns:
            time_data[time_column] = time_data[time_column].astype(np.int32)

        return time_columns, time_data[time_columns]

class MeanExtractor(BaseExtractor):
    def extract(self, all_data):
        mean_columns = [
            'cummean_total_score_home_match_team_home', 'cummean_total_score_away_match_team_away',
            'cummean_w_prob_home_match_team_home', 'cummean_d_prob_home_match_team_home', 'cummean_l_prob_home_match_team_home',
            'cummean_w_prob_away_match_team_away', 'cummean_d_prob_away_match_team_away', 'cummean_l_prob_away_match_team_away'
        ]

        mean_data = pd.DataFrame(index = all_data.index)
        mean_data[['team_home', 'team_away']] = all_data[['team_home', 'team_away']]
        mean_data['cummean_total_score_home_match_team_home'] = (all_data.groupby('team_home')['score_home'].cumsum() - all_data['score_home']) / all_data.groupby('team_home')['score_home'].cumcount()
        mean_data['cummean_total_score_away_match_team_away'] = (all_data.groupby('team_away')['score_away'].cumsum() - all_data['score_away']) / all_data.groupby('team_away')['score_away'].cumcount()

        mean_data['team_home_w'] = (all_data['real_outcome_o1x2'] == 'o1').astype(np.int32)
        mean_data['team_home_d'] = (all_data['real_outcome_o1x2'] == 'o0').astype(np.int32)
        mean_data['team_home_l'] = (all_data['real_outcome_o1x2'] == 'o2').astype(np.int32)
        mean_data['team_away_w'] = (all_data['real_outcome_o1x2'] == 'o2').astype(np.int32)
        mean_data['team_away_d'] = (all_data['real_outcome_o1x2'] == 'o0').astype(np.int32)
        mean_data['team_away_l'] = (all_data['real_outcome_o1x2'] == 'o1').astype(np.int32)

        mean_data['cummean_w_prob_home_match_team_home'] = (mean_data.groupby('team_home')['team_home_w'].cumsum() - mean_data['team_home_w']) / mean_data.groupby('team_home')['team_home_w'].cumcount()
        mean_data['cummean_d_prob_home_match_team_home'] = (mean_data.groupby('team_home')['team_home_d'].cumsum() - mean_data['team_home_d']) / mean_data.groupby('team_home')['team_home_d'].cumcount()
        mean_data['cummean_l_prob_home_match_team_home'] = (mean_data.groupby('team_home')['team_home_l'].cumsum() - mean_data['team_home_l']) / mean_data.groupby('team_home')['team_home_l'].cumcount()
        mean_data['cummean_w_prob_away_match_team_away'] = (mean_data.groupby('team_away')['team_away_w'].cumsum() - mean_data['team_away_w']) / mean_data.groupby('team_away')['team_away_w'].cumcount()
        mean_data['cummean_d_prob_away_match_team_away'] = (mean_data.groupby('team_away')['team_away_d'].cumsum() - mean_data['team_away_d']) / mean_data.groupby('team_away')['team_away_d'].cumcount()
        mean_data['cummean_l_prob_away_match_team_away'] = (mean_data.groupby('team_away')['team_away_l'].cumsum() - mean_data['team_away_l']) / mean_data.groupby('team_away')['team_away_l'].cumcount()

        for mean_column in mean_columns:
            mean_data[mean_column] = mean_data[mean_column].astype(np.float32)

        return mean_columns, mean_data[mean_columns]

class MatchNumExtractor(BaseExtractor):
    def __init__(self, teams):
        self.teams = teams
    def extract(self, all_data):
        match_num_columns = [
            'any_season_any_match_team_home_match_num', 'any_season_any_match_team_away_match_num',
            'current_season_any_match_team_home_match_num', 'current_season_any_match_team_away_match_num'
        ]
        match_num_data = pd.DataFrame(index = all_data.index)
        for team_name in self.teams:
            team_data = all_data[(all_data.team_home == team_name) | (all_data.team_away == team_name)]
            team_data['match_num'] = np.arange(len(team_data))
            team_home_index = team_data[team_data['team_home'] == team_name].index
            team_away_index = team_data[team_data['team_away'] == team_name].index
            match_num_data.loc[team_home_index, 'any_season_any_match_team_home_match_num'] = team_data['match_num']
            match_num_data.loc[team_away_index, 'any_season_any_match_team_away_match_num'] = team_data['match_num']

        for season in all_data['season'].unique():
            season_data = all_data[all_data.season == season]
            for team_name in self.teams:
                team_data = season_data[(season_data.team_home == team_name) | (season_data.team_away == team_name)]
                team_data['match_num'] = np.arange(len(team_data))
                team_home_index = team_data[team_data['team_home'] == team_name].index
                team_away_index = team_data[team_data['team_away'] == team_name].index
                match_num_data.loc[team_home_index, 'current_season_any_match_team_home_match_num'] = team_data['match_num']
                match_num_data.loc[team_away_index, 'current_season_any_match_team_away_match_num'] = team_data['match_num']

        for match_num_column in match_num_columns:
            match_num_data[match_num_column] = match_num_data[match_num_column].astype(np.int32)
        
        return match_num_columns, match_num_data[match_num_columns]

class LastResultsExtractor(BaseExtractor):
    def __init__(self, teams):
        self.teams = teams
    def extract(self, all_data):
        team_results = pd.DataFrame(index = all_data.index, columns=['any_match_team_home_results', 'any_match_team_away_results'])
        for team_name in self.teams:
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

        for results_column in results_columns:
            team_results_data[results_column] = team_results_data[results_column].astype(np.int32)

        return results_columns, team_results_data[results_columns]

class TimeLagsExtractor(BaseExtractor):
    def __init__(self, teams):
        self.teams = teams
    def extract(self, all_data):
        max_lag = 10
        lags = range(1, max_lag + 1)
        lag_names = ['lag_' + str(lag) for lag in lags]
        time_lags_columns = ['diff_start_time_any_match_' + ''.join(t) for t in list(product(['team_home_', 'team_away_'], lag_names))]
        time_lags_data = pd.DataFrame(index=all_data.index, dtype=np.float64, columns=time_lags_columns)
        for team_name in self.teams:
            for lag in lags:
                lag_name = 'lag_' + str(lag)
                team_data = all_data[(all_data['team_home'] == team_name) | (all_data['team_away'] == team_name)]
                time_any_match = team_data['time'].diff(lag) / (24 * 60 * 60)
                team_home_index = team_data[team_data['team_home'] == team_name].index
                team_away_index = team_data[team_data['team_away'] == team_name].index
                time_lags_data.loc[team_home_index, 'diff_start_time_any_match_team_home_' + lag_name] = time_any_match.loc[team_home_index]
                time_lags_data.loc[team_away_index, 'diff_start_time_any_match_team_away_' + lag_name] = time_any_match.loc[team_away_index]

        for time_lags_column in time_lags_columns:
            time_lags_data[time_lags_column] = time_lags_data[time_lags_column].astype(np.float32)      

        return time_lags_columns, time_lags_data[time_lags_columns]

class OutcomeLagsExtractor(BaseExtractor):
    def __init__(self, teams):
        self.teams = teams
    def extract(self, all_data):
        max_lag = 10
        lags = range(1, max_lag + 1)
        lag_names = ['lag_' + str(lag) for lag in lags]
        outcome_lags_columns = ['outcome_any_match_' + ''.join(t) for t in list(product(['team_home_', 'team_away_'], lag_names))]
        outcome_lags_data = pd.DataFrame(index=all_data.index, dtype=np.int32, columns=outcome_lags_columns)
        for team_name in self.teams:
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

        for outcome_lags_column in outcome_lags_columns:
            outcome_lags_data[outcome_lags_column] = outcome_lags_data[outcome_lags_column].map({np.nan: 1, np.inf: 1, 'w': 0, 'd': 1, 'l': 2}).astype(np.int32)

        return outcome_lags_columns, outcome_lags_data[outcome_lags_columns]

class BookExtractor(BaseExtractor):
    def __init__(self, book_name, original_data):
        self.book_name = book_name
        self.opening_normalized_probs = 1. / original_data[[f'o1x2_{book_name}_opening_o1_coef', f'o1x2_{book_name}_opening_o0_coef', f'o1x2_{book_name}_opening_o2_coef']]
        self.closing_normalized_probs = 1. / original_data[[f'o1x2_{book_name}_closing_o1_coef', f'o1x2_{book_name}_closing_o0_coef', f'o1x2_{book_name}_closing_o2_coef']]
    def __get_prob_by_coef(self, coef):
        if coef == 0.0:
            return 0.0
        return 1 / coef
    # def __extract_time_features(self, all_data_prepared):
    #     name = self.book_name
    #     def time_diff(row):
    #         value = row[f'o1x2_{name}_opening_time']
    #         if not value or np.isnan(value) or np.isinf(value):
    #             return min_int
    #         else:
    #             return row['o1x2_pin_opening_time'] - value
    #     all_data_prepared[f'pin_{name}_opening_time_diff'] = all_data_prepared.apply(time_diff, axis=1)
    #     all_data_prepared[f'pin_{name}_opening_time_diff'] = all_data_prepared[f'pin_{name}_opening_time_diff'].astype(np.int32)
    #     return [f'pin_{name}_opening_time_diff'], all_data_prepared
    def extract(self, all_data):
        name = self.book_name
        book_data = pd.DataFrame(index = all_data.index)
        book_data[f'{name}_opening_coef'] = all_data.apply(lambda row: row[f'o1x2_{name}_opening_{row["outcome_o1x2"]}_coef'], axis=1)
        book_data[f'{name}_closing_coef'] = all_data.apply(lambda row: row[f'o1x2_{name}_closing_{row["outcome_o1x2"]}_coef'], axis=1)
        book_data[f'{name}_opening_coef'] = all_data[f'{name}_opening_coef'].astype(np.float32)
        book_data[f'{name}_closing_coef'] = all_data[f'{name}_closing_coef'].astype(np.float32)
        
        book_data[f'{name}_opening_prob'] = all_data[f'{name}_opening_coef'].apply(self.__get_prob_by_coef)
        book_data[f'{name}_closing_prob'] = all_data[f'{name}_closing_coef'].apply(self.__get_prob_by_coef)
        book_data[f'{name}_opening_prob'] = all_data[f'{name}_opening_prob'].astype(np.float32)
        book_data[f'{name}_closing_prob'] = all_data[f'{name}_closing_prob'].astype(np.float32)

        book_data[f'{name}_opening_prob_norm'] = np.hstack(self.opening_normalized_probs.div(self.opening_normalized_probs.sum(axis=1), axis=0).values)
        book_data[f'{name}_closing_prob_norm'] = np.hstack(self.closing_normalized_probs.div(self.closing_normalized_probs.sum(axis=1), axis=0).values)
        book_data[f'{name}_opening_prob_norm'] = all_data[f'{name}_opening_prob_norm'].astype(np.float32)
        book_data[f'{name}_closing_prob_norm'] = all_data[f'{name}_closing_prob_norm'].astype(np.float32)

        #time_columns, all_data_prepared = self.__extract_time_features(all_data)

        book_columns = [f'{name}_opening_coef', f'{name}_closing_coef', f'{name}_opening_prob', f'{name}_closing_prob', f'{name}_opening_prob_norm', f'{name}_closing_prob_norm']# + time_columns
        return book_columns, book_data[book_columns]

class PinExtractor(BookExtractor):
    def __init__(self, original_data):
        super().__init__('pin', original_data)
    # def __extract_time_features(self, all_data):
    #     return [], pd.DataFrame()
