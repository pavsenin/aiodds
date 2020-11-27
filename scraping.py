import json
from datetime import datetime
from urllib.parse import unquote

from constants import soccer_min_year, fetch_max_retry, bookmakers_map
from constants import out_1x2_id, out_ou_id, out_ah_id, soccer_id, soccer_data_id
from utils import retry, get_timestamp, parse_tree, fetch_url, fetch_url_oddsportal

class MatchScraper:
    @staticmethod
    def correct_score(score, text):
        index = score.find(text)
        if index < 0:
            return score, False
        score = score[:index].strip()
        return score, True
    @staticmethod
    def parse_score(text):
        parsed = text.split(':')
        if len(parsed) != 2:
            return None, None
        return int(parsed[0]), int(parsed[1])
    @staticmethod
    def fetch_odds(odds_data):
        odds_data_url = f'https://fb.oddsportal.com{odds_data}?_={get_timestamp()}'
        data_content = fetch_url_oddsportal(odds_data_url)
        start_text, end_text = f"globals.jsonpCallback('{odds_data}', ", ");"
        i1, i2 = data_content.find(start_text) + len(start_text), data_content.rfind(end_text)
        json_value = json.loads(data_content[i1:i2])
        json_value = json_value['d']
        if 'oddsdata' not in json_value:
            return None
        json_value = json_value['oddsdata']
        if 'back' not in json_value:
            return None
        json_value = json_value['back']
        return json_value
    @staticmethod
    def fill_info_x3(info, data, book, key):
        o1, o0, o2 = None, None, None
        if book in data:
            data_book = data[book]
            if isinstance(data_book, list):
                o1, o0, o2 = data_book[0], data_book[1], data_book[2]
            else:
                o1, o0, o2 = data_book['0'], data_book['1'], data_book['2']
        info[f'{key}_o1_coef'], info[f'{key}_o0_coef'], info[f'{key}_o2_coef'] = o1, o0, o2
    @staticmethod
    def fill_info_x2(info, data, book, key):
        o1, o2 = None, None
        if book in data:
            data_book = data[book]
            if isinstance(data_book, list):
                o1, o2 = data_book[0], data_book[1]
            else:
                o1, o2 = data_book['0'], data_book['1']
        info[f'{key}_o1_coef'], info[f'{key}_o2_coef'] = o1, o2
    @staticmethod
    def fill_info(out_kind, info, data, book, key):
        if out_kind == 'x2':
            return MatchScraper.fill_info_x2(info, data, book, key)
        else:
            return MatchScraper.fill_info_x3(info, data, book, key)
    @staticmethod
    def fill_info_time(info, data, book, key):
        odd_time = None
        if book in data:
            data_book = data[book]
            if isinstance(data_book, list):
                odd_times = data_book
            else:
                odd_times = data_book.values()
            odd_times = [t for t in odd_times if t is not None]
            if odd_times:
                odd_time = max(odd_times)
        info[f'{key}_time'] = odd_time

    def __do_time_filter(self, time):
        if str(time.year) < soccer_min_year:
            return True
        return False

    def __scrap_odds(self, info, match_id, xhash):
        for out_id, out_kind, handicap, bet_name in [(out_1x2_id, 'x3', 0, 'o1x2'), (out_ou_id, 'x2', 2.5, 'ou25'), (out_ah_id, 'x2', -0.5, 'ahmin05')]:
            match_data = f'/feed/match/1-{soccer_id}-{match_id}-{out_id}-{soccer_data_id}-{xhash}.dat'
            json_value = retry(self.fetch_odds, match_data, fetch_max_retry)
            out_key = f'E-{out_id}-{soccer_data_id}-0-{handicap}-0'
            opening_odds, opening_change_time, closing_odds, closing_change_time = {}, {}, {}, {}
            if json_value is not None and out_key in json_value:
                json_value = json_value[out_key]
                opening_odds, opening_change_time, closing_odds, closing_change_time = \
                    json_value['opening_odds'], json_value['opening_change_time'], json_value['odds'], json_value['change_time']

            for book, book_name in bookmakers_map.items():
                self.fill_info(out_kind, info, opening_odds, book, f'{bet_name}_{book_name}_opening')
                self.fill_info(out_kind, info, closing_odds, book, f'{bet_name}_{book_name}_closing')
                self.fill_info_time(info, opening_change_time, book, f'{bet_name}_{book_name}_opening')
                self.fill_info_time(info, closing_change_time, book, f'{bet_name}_{book_name}_closing')

    def scrap(self, country, league, matchID):
        match_url = f'https://www.oddsportal.com/soccer/{country}/{league}/{matchID}/'
        content = fetch_url(match_url)
        
        tree = parse_tree(content)
        xhash_text = str(tree.find('html').find('body').find('script'))
        xhash_label = '"xhash":"'
        i1, i2 = xhash_text.find(xhash_label) + len(xhash_label), xhash_text.find('","xhashf"')
        if i1 < 0 or i2 < i1:
            return None

        xhash = unquote(xhash_text[i1:i2])
        main_node = tree.find('html').find('body').find('div').find('div', {'id':'mother-main'}).find('div', {'id':'mother'}).find('div', {'id':'wrap'}).find('div').find('div').find('div', {'id':'main'}).find('div', {'id':'col-content'})

        time = int(main_node.find('p')['class'][2][1:11])
        time_dt = datetime.utcfromtimestamp(time)
        if self.__do_time_filter(time_dt):
            return None

        teams_text = main_node.find('h1').text
        teams = [team.strip() for team in teams_text.split('-')]
        team_home, team_away = teams[0], teams[1]

        score_node = main_node.find('div', {'id':'event-status'}).find('strong')
        was_extra = False
        if score_node is None:
            score_home, score_away = None, None
        else:
            score = score_node.text
            score, was_pen = self.correct_score(score, 'penalties')
            score, was_et = self.correct_score(score, 'ET')
            score, was_ot = self.correct_score(score, 'OT')
            score_home, score_away = self.parse_score(score)
            was_extra = was_pen or was_et or was_ot

        periods_node = main_node.find('div', {'id':'event-status'}).find('p', {'class':'result'})
        period1_home, period1_away, period2_home, period2_away = None, None, None, None
        if periods_node is not None:
            periods_text = periods_node.text
            i1, i2 = periods_text.find('(') + 1, periods_text.find(')')
            periods_text = periods_text[i1:i2]
            periods = [period.strip() for period in periods_text.split(',')]
            if len(periods) >= 2:
                (period1_home, period1_away), (period2_home, period2_away) = self.parse_score(periods[0]), self.parse_score(periods[1])

        match_info = {
            'match_id': matchID,
            'time': time,
            'team_home': team_home,
            'team_away': team_away,
            'score_home': score_home,
            'score_away': score_away,
            'was_extra': was_extra,
            'score_home_period1': period1_home,
            'score_away_period1': period1_away,
            'score_home_period2': period2_home,
            'score_away_period2': period2_away
        }
        self.__scrap_odds(match_info, matchID, xhash)
        return match_info

class FutureMatchScraper(MatchScraper):
    def __init__(self, from_time, to_time):
        self.from_time = from_time
        self.to_time = to_time
    def __do_time_filter(self, time):
        if super().__do_time_filter(time):
            return True
        if self.from_time and time < self.from_time:
            return True
        if self.to_time and time > self.to_time:
            return True
        return False