import numpy as np
import pandas as pd

from utils import get_outcome_o1x2, equal
from db import DBProvider
from feature_extracting import TimeExtractor, PinExtractor, MeanExtractor

def select_matches(match_ids):
    db = DBProvider()
    ids = db.join_values(match_ids)
    request = f"SELECT l.season, l.country, l.league_name, m.* FROM matches m JOIN leagues l ON l.id = m.league_id WHERE m.match_id IN ({ids}) ORDER BY m.time, l.country, l.league_name"
    matches_rows = db.select(request)
    return pd.DataFrame(data=matches_rows, columns=list(matches_rows[0].keys()))

def test_time_extractor_columns():
    all_data = select_matches(['W8tiZmre'])
    extractor = TimeExtractor()
    columns, _ = extractor.extract(all_data)
    assert columns == ['year', 'month', 'day', 'hour', 'weekday']

def test_time_extractor_data():
    all_data = select_matches(['W8tiZmre'])
    extractor = TimeExtractor()
    _, extracted_data = extractor.extract(all_data)
    assert extracted_data.loc[0, 'year'] == 2013
    assert extracted_data.loc[0, 'month'] == 9
    assert extracted_data.loc[0, 'day'] == 1
    assert extracted_data.loc[0, 'hour'] == 16
    assert extracted_data.loc[0, 'weekday'] == 6

def test_mean_extractor_columns():
    all_data = select_matches(['W8tiZmre'])
    all_data['real_outcome_o1x2'] = all_data.apply(get_outcome_o1x2, axis=1)
    extractor = MeanExtractor()
    columns, _ = extractor.extract(all_data)
    assert columns == [
        'cummean_total_score_home_match_team_home', 'cummean_total_score_away_match_team_away',
        'cummean_w_prob_home_match_team_home', 'cummean_d_prob_home_match_team_home', 'cummean_l_prob_home_match_team_home',
        'cummean_w_prob_away_match_team_away', 'cummean_d_prob_away_match_team_away', 'cummean_l_prob_away_match_team_away'
    ]

def test_mean_extractor_data():
    # Liverpool - Manchester Utd 1:0 (1:0, 0:0)
    # Swansea - Liverpool 2:2 (1:2, 1:0)
    # Liverpool - Southampton 0:1 (0:0, 0:1)
    # Sunderland - Liverpool 1:3 (0:2, 1:1)
    # Liverpool - Crystal Palace 3:1 (3:0, 0:1)
    # Newcastle - Liverpool  2:2 (1:1, 1:1)
    # Liverpool - West Brom  4:1 (2:0, 2:1)
    # Arsenal - Liverpool 2:0 (1:0, 1:0)
    # Liverpool - Fulham 4:0 (3:0, 1:0)
    all_data = select_matches(['W8tiZmre', 'b5ZpFZlH', 'Y7HP6S3Q', 'KSHwEqBg', 'l89fA1AI', 'plSc5COj', 'tzC0PXFM', '8MZNtZpc', '0b78hWq9'])
    all_data['real_outcome_o1x2'] = all_data.apply(get_outcome_o1x2, axis=1)
    extractor = MeanExtractor()
    _, extracted_data = extractor.extract(all_data)

    expected = [
        (np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan),
        (np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan),
        (1,      np.nan, 1,      0,      0,      np.nan, np.nan, np.nan),
        (np.nan, 2,      np.nan, np.nan, np.nan, 0,      1,      0     ),
        (1 / 2,  np.nan, 1 / 2,  0,      1 / 2,  np.nan, np.nan, np.nan),
        (np.nan, 5 / 2,  np.nan, np.nan, np.nan, 1 / 2,  1 / 2,  0     ),
        (4 / 3,  np.nan, 2 / 3,  0,      1 / 3,  np.nan, np.nan, np.nan),
        (np.nan, 7 / 3,  np.nan, np.nan, np.nan, 1 / 3,  2 / 3,  0),
        (2,      np.nan, 3 / 4,  0,      1 / 4,  np.nan, np.nan, np.nan),
    ]
    for i, (total_score_home, total_score_away, w_prob_home, d_prob_home, l_prob_home, w_prob_away, d_prob_away, l_prob_away) in enumerate(expected):
        equal(extracted_data.loc[i, 'cummean_total_score_home_match_team_home'], total_score_home) 
        equal(extracted_data.loc[i, 'cummean_total_score_away_match_team_away'], total_score_away)
        equal(extracted_data.loc[i, 'cummean_w_prob_home_match_team_home'], w_prob_home)
        equal(extracted_data.loc[i, 'cummean_d_prob_home_match_team_home'], d_prob_home)
        equal(extracted_data.loc[i, 'cummean_l_prob_home_match_team_home'], l_prob_home)
        equal(extracted_data.loc[i, 'cummean_w_prob_away_match_team_away'], w_prob_away)
        equal(extracted_data.loc[i, 'cummean_d_prob_away_match_team_away'], d_prob_away)
        equal(extracted_data.loc[i, 'cummean_l_prob_away_match_team_away'], l_prob_away)