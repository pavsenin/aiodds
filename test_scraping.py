from datetime import datetime, timedelta

from scraping import MatchScraper

def test_scrap_match():
    country, league, match_id = 'england', 'premier-league', 'drtdPJU9'
    scraper = MatchScraper(from_time=None, to_time=None)
    match_info = scraper.scrap(country, league, match_id)

    assert match_info['match_id'] == match_id
    assert datetime.fromtimestamp(match_info['time']) == datetime(2020, 11, 8, 22, 15)
    assert match_info['team_home'] == 'Arsenal'
    assert match_info['team_away'] == 'Aston Villa'
    assert match_info['score_home'] == 0
    assert match_info['score_away'] == 3
    assert match_info['was_extra'] == False
    assert match_info['score_home_period1'] == 0
    assert match_info['score_away_period1'] == 1
    assert match_info['score_home_period2'] == 0
    assert match_info['score_away_period2'] == 2

    assert match_info['o1x2_pin_opening_o1_coef'] == 1.59
    assert match_info['o1x2_pin_opening_o0_coef'] == 4.44
    assert match_info['o1x2_pin_opening_o2_coef'] == 5.75
    assert match_info['o1x2_pin_closing_o1_coef'] == 1.66
    assert match_info['o1x2_pin_closing_o0_coef'] == 4.58
    assert match_info['o1x2_pin_closing_o2_coef'] == 4.95
    assert datetime.fromtimestamp(match_info['o1x2_pin_opening_time']) == datetime(2020, 10, 30, 12, 21, 48)
    assert datetime.fromtimestamp(match_info['o1x2_pin_closing_time']) == datetime(2020, 11, 8, 22, 14, 18)
    assert match_info['ou25_pin_opening_o1_coef'] == 1.67
    assert match_info['ou25_pin_opening_o2_coef'] == 2.33
    assert match_info['ou25_pin_closing_o1_coef'] == 1.61
    assert match_info['ou25_pin_closing_o2_coef'] == 2.47
    assert datetime.fromtimestamp(match_info['ou25_pin_opening_time']) == datetime(2020, 10, 30, 12, 21, 48)
    assert datetime.fromtimestamp(match_info['ou25_pin_closing_time']) == datetime(2020, 11, 8, 22, 14, 18)
    assert match_info['ahmin05_pin_opening_o1_coef'] == 1.58
    assert match_info['ahmin05_pin_opening_o2_coef'] == 2.53
    assert match_info['ahmin05_pin_closing_o1_coef'] == 1.66
    assert match_info['ahmin05_pin_closing_o2_coef'] == 2.39
    assert datetime.fromtimestamp(match_info['ahmin05_pin_opening_time']) == datetime(2020, 10, 30, 12, 21, 48)
    assert datetime.fromtimestamp(match_info['ahmin05_pin_closing_time']) == datetime(2020, 11, 8, 22, 12, 18)

# def test_scrap_league():
#     pass