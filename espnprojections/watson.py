import json
import logging

import browser_cookie3
import numpy as np
import pandas as pd
from requests_cache import CachedSession

import nflnames
from nflprojections import ProjectionSource


class Scraper:

    HEADERS = {
        'sec-ch-ua': '"Chromium";v="94", "Google Chrome";v="94", ";Not A Brand";v="99"',
        'Accept': 'application/json, text/plain, */*',
        'Referer': 'https://watsonfantasyfootball.espn.com/espnpartner/playercard?player1=2330&leagueId=1651700&teamId=4&view=desktop&leagueType=nonppr&actual=--&projected=19&avg=28&',
        'DNT': '1',
        'sec-ch-ua-mobile': '?0',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.54 Safari/537.36',
        'sec-ch-ua-platform': '"Linux"',
    }

    def __init__(self, season, headers=None, cookies=None):
        self._s = CachedSession('http_cache', backend='sqlite', use_cache_dir=True)
        self.season = season
        self.headers = headers if headers else self.HEADERS
        self.cookies = cookies if cookies else browser_cookie3.firefox()

    @property
    def base_url(self):
        return 'https://watsonfantasyfootball.espn.com/espnpartner/dallas/'

    def get(self, url):
        return self._s.get(url).json()
        
    def performance(self, player_id):
        """Gets Watson performance resource for single player"""
        url = f'performance/performance_{player_id}_ESPNFantasyFootball_{self.season}.json'
        return self.get(self.base_url + url)

    def player(self, player_id):
        """Gets single Watson player"""
        url = f'players/players_{player_id}_ESPNFantasyFootball_{self.season}.json'
        return self.get(self.base_url + url)

    def players(self):
        """Gets list of all Watson players"""
        url = 'players/players_ESPNFantasyFootball_{self.season}.json'
        return self.get(self.base_url + url)

    def playertrend(self, player_id):
        """Gets Watson playertrend for single player"""
        url = f'playertrends/playertrends_{player_id}_ESPNFantasyFootball_{self.season}.json'
        return self.get(self.base_url + url)

    def projection(self, player_id):
        """Gets Watson projection for single player"""
        url = f'projections/projections_{player_id}_ESPNFantasyFootball_{self.season}.json'
        return self.get(self.base_url + url)


class Parser:
    """Parse Watson projections
    
    TODO: need to distinguish all vs. most recent resource
    """

    PLAYER_KEYS = ['ACTUAL', 'DATA_TIMESTAMP', 'SET_END', 'EVENT_WEEK', 'OPPONENT_NAME', 'OPPOSITION_RANK', 'PLAYERID', 'FANTASY_PLAYER_ID', 
                   'EVENT_YEAR', 'FULL_NAME', 'POSITION', 'TEAM', 'TEAM_LOCATION', 'AGE', 'HEIGHT', 'WEIGHT', 'YEARS_EXPERIENCE', 'PRO_TEAM_ID', 
                   'IS_ON_INJURED_RESERVE', 'IS_SUSPENDED', 'IS_ON_BYE', 'IS_FREE_AGENT', 'CURRENT_RANK', 'INJURY_STATUS_DATE', 'OUTSIDE_PROJECTION']

    def __init__(self):
        logging.getLogger(__name__).addHandler(logging.NullHandler())
    
    def performance(self, data):
        """Parses performance JSON"""
        return data

    def player(self, data):
        """Parses player JSON"""
        if isinstance(data, list):
            data = data[-1]
        return {k: data[k] for k in self.PLAYER_KEYS}

    def players(self, data):
        """Parses players JSON"""
        return [self.player(item) for item in data]

    def playertrend(self, data):
        """Parses playertrend JSON"""
        return data

    def projection(self, data):
        """Parses projection JSON"""
        wanted = ['PLAYERID', 'DATA_TIMESTAMP', 'SCORE_PROJECTION', 'SCORE_DISTRIBUTION', 'LOW_SCORE',
                  'HIGH_SCORE', 'OUTSIDE_PROJECTION', 'SIMULATION_PROJECTION']
        return {k: v for k, v in data[-1].items() if k in wanted}

    def projection_distribution(self, data):
        '''
        Returns distribution of scores for player

        Returns:
            list: of float

        '''
        try:
            return [s[0] for s in json.loads(data['SCORE_DISTRIBUTION'])]
        except:
            return None

    def randomize_watson(projections, percentiles=(25, 75)):
        """
        Uses range of projections to optimize

        """
        def _get_proj(player):
            # score_distribution is a list of lists
            # can use zip with * operator to unpack
            new_player = {k: v for k,v in player.items() if k != 'score_distribution'}  
            scores, _ = zip(*player['score_distribution'])
            pctfloor, pctceil = np.percentile(scores, percentiles)
            score_range = [score for score in scores if score >= pctfloor and score <= pctceil]
            new_player['dist'] = np.random.choice(score_range)
            return new_player

        return [_get_proj(player) for player in projections]


class WatsonProjections(ProjectionSource):
    """Standardizes Waston projections from espn.com"""

    COLUMN_MAPPING = {
        'EVENT_WEEK': 'week',
        'OPPONENT_NAME': 'opp',
        'EVENT_YEAR': 'season',
        'FULL_NAME': 'plyr',
        'POSITION': 'pos',
        'TEAM': 'team',
        'OUTSIDE_PROJECTION': 'outside_projection',
        'SCORE_PROJECTION': 'proj', 
        'SCORE_DISTRIBUTION': 'score_distribution', 
        'LOW_SCORE': 'low_score', 
        'HIGH_SCORE': 'high_score', 
        'SIMULATION_PROJECTION': 'simulation_projection'
    }

    def __init__(self, season: int, **kwargs):
        """Creates object"""
        kwargs['column_mapping'] = self.COLUMN_MAPPING
        kwargs['projections_name'] = 'watson'
        super().__init__(**kwargs)
        self.season = season

    def load_raw(self):
        values = []
        s = Scraper(season=self.season)
        p = Parser()
        for item in s.players(self.season):
            player = p.player(item)
            proj = s.projection(player['PLAYER_ID'])           
            values.append({**player, **proj})
        return values
        
    def process_raw(self, df):
        """Processes raw dataframe"""
        df.columns = self.remap_columns(df.columns)
        wanted = list(self.COLUMN_MAPPING.values())
        return df.loc[:, wanted]

    def standardize(self, df):
        """Standardizes names/teams/positions
        
        Args:
            df (pd.DataFrame): the projections dataframe

        Returns
            pd.DataFrame

        """
        # standardize team and opp
        df = df.assign(team=self.standardize_teams(df.team), opp=self.standardize_teams(df.opp))

        # standardize positions
        df = df.assign(pos=self.standardize_positions(df.pos))

        # standardize player names
        return df.assign(plyr=lambda x: self.standardize_players(x))

    def standardize_players(self, df: pd.DataFrame) -> pd.Series:
        """Standardizes player names
        
        Args:
            df (pd.DataFrame): the projections dataframe

        Returns:
            pd.Series

        """
        # different approach for defenses
        # different rules for defense and players
        return np.where(df.pos == 'DST', 
                        df.team.str.lower() + ' defense', 
                        df.plyr.apply(nflnames.standardize_player_name))
