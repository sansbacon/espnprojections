import json
import logging
import random

import browser_cookie3
import numpy as np
import pandas as pd
import requests

import nflnames
from nflprojections import ProjectionSource


class Scraper:

    HEADERS = {
        'authority': 'watsonfantasyfootball.espn.com',
        'sec-ch-ua': '"Chromium";v="92", " Not A;Brand";v="99", "Google Chrome";v="92"',
        'accept': 'application/json, text/plain, */*',
        'dnt': '1',
        'sec-ch-ua-mobile': '?0',
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-mode': 'cors',
        'sec-fetch-dest': 'empty',
        'referer': 'https://watsonfantasyfootball.espn.com/espnpartner/playercard?player1=2330&leagueId=1651700&teamId=4&view=desktop&leagueType=nonppr&actual=--&projected=20&avg=27&',
        'accept-language': 'en-US,en;q=0.9,ar;q=0.8',
    }

    def __init__(self, season, headers=None, cookies=None):
        logging.getLogger(__name__).addHandler(logging.NullHandler())
        self._s = requests.Session()       
        self.season = season
        self.headers = headers if headers else self.HEADERS
        self.cookies = cookies if cookies else browser_cookie3.firefox()

    @property
    def base_url(self):
        return 'https://watsonfantasyfootball.espn.com/espnpartner/dallas/'

    def get(self, url):
        return self._s.get(url)
        
    def player(self, player_id):
        """Gets single Watson player"""
        url = f'players/players_{player_id}_ESPNFantasyFootball_{self.season}.json'
        return self.get(self.base_url + url)

    def players(self):
        """Gets list of all Watson players"""
        url = 'players/players_ESPNFantasyFootball_{self.season}.json'
        return self.get(self.base_url + url)
       
    def projection(self, player_id):
        """Gets Watson projection for single player"""
        url = f'projections/projections_{player_id}_ESPNFantasyFootball_{self.season}.json'
        return self.get(self.base_url + url)


class Parser:
    """Parse Watson projections"""

    def __init__(self):
        logging.getLogger(__name__).addHandler(logging.NullHandler())
    
    def projection(self, player):
        """Parses player JSON"""
        pass

    def projection_distribution(self, player):
        '''
        Returns distribution of scores for player

        Returns:
            list: of float

        '''
        try:
            return [s[0] for s in json.loads(player['SCORE_DISTRIBUTION'])]
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
            scores, probs = zip(*player['score_distribution'])
            pctfloor, pctceil = np.percentile(scores, percentiles)
            score_range = [score for score in scores if score >= pctfloor and score <= pctceil]
            new_player['dist'] = random.choice(score_range)
            return new_player

        return [_get_proj(player) for player in projections]


class WatsonProjections(ProjectionSource):
    """Standardizes Waston projections from espn.com"""

    COLUMN_MAPPING = {
        'DK Position': 'pos',
        'DK Salary': 'salary',
        'DK Projection': 'proj',
        'DK Value': 'value',
        'DK Ownership': 'ownership',
        'DKSlateID': 'dk_slate_id',
        'Opponent': 'opp',
        'Team': 'team',
        'Player': 'plyr',
        'DK Ceiling': 'ceil',
        'DK Floor': 'floor'
    }

    def __init__(self, **kwargs):
        """Creates object"""
        kwargs['column_mapping'] = self.COLUMN_MAPPING
        kwargs['projections_name'] = 'etr'
        super().__init__(**kwargs)

    def load_raw(self):
        s = Scraper()
        p = Parser()
        return p.projections(s.get_projections(self.site_name, self.slate_name))

    def process_raw(self, df):
        """Processes raw dataframe"""
        df.columns = self.remap_columns(df.columns)
        if df['ownership'].dtype == str:
            df['ownership'] = df['ownership'].str.replace('%', '').astype(int)
        if df['salary'].dtype == str:
            df['salary'] = pd.to_numeric(df['salary'].str.replace('$', ''), errors='coerce').fillna(0).astype(int)
        df = df.dropna(thresh=7)
        df.loc[:, 'dk_slate_id'] = df.loc[:, 'dk_slate_id'].astype(int)
        return df

    def standardize(self, df):
        """Standardizes names/teams/positions
        
        Args:
            df (pd.DataFrame): the projections dataframe

        Returns
            pd.DataFrame

        """
        # standardize team and opp
        df = df.assign(team=self.standardize_teams(df.team.replace({'LA': 'LAR'})), opp=self.standardize_teams(df.opp.replace({'LA': 'LAR'})))

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
                        df.pos.apply(nflnames.standardize_defense_name), 
                        df.pos.apply(nflnames.standardize_player_name))