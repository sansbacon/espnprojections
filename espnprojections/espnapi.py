"""
espnapi.py
classes for scraping, parsing espn football api
this includes fantasy and real nfl data

Usage:

    import nflprojections.espnapi as espn
    
    season = 2020
    week = 1
    s = espn.Scraper(season=season)
    p = espn.Parser(season=season)
    data = s.playerstats(season)
    print(p.weekly_projections(data, week))

"""

import json
import logging
from pathlib import Path
import time

from requests_html import HTMLSession


class Scraper:
    """
    Scrape ESPN API for football stats

    """

    def __init__(self, season):
        """Creates Scraper instance"""
        self.season = season
        self._s = HTMLSession()

    @property
    def api_url(self):
        return f"https://fantasy.espn.com/apis/v3/games/ffl/seasons/{self.season}/segments/0/leaguedefaults/3"

    @property
    def default_headers(self):
        return {
            "authority": "fantasy.espn.com",
            "accept": "application/json",
            "x-fantasy-source": "kona",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.102 Safari/537.36",
            "x-fantasy-platform": "kona-PROD-a9859dd5e813fa08e6946514bbb0c3f795a4ea23",
            "dnt": "1",
            "sec-fetch-site": "same-origin",
            "sec-fetch-mode": "cors",
            "sec-fetch-dest": "empty",
            "referer": "https://fantasy.espn.com/football/players/projections",
            "accept-language": "en-US,en;q=0.9,ar;q=0.8",
            "if-none-match": "W/^\\^008956866aeb5b199ec8612a9e7576ed7^\\^",
            "x-fantasy-filter": json.dumps(self.xff)
        }

    @property
    def default_params(self):
        return {"view": "kona_player_info"}

 
    @property
    def xff(self):
        """Default x-fantasy-filter"""
        return {
            "players": {
                "limit": 1500,
                "sortDraftRanks": {
                    "sortPriority": 100,
                    "sortAsc": True,
                    "value": "PPR",
                }
            }
        }

    def get_json(self, url, headers, params, return_object=False):
        """Gets json response"""
        r = self._s.get(url, headers=headers, params=params)
        if return_object:
            return r
        return r.json()

    def projections(self):
        """Gets all ESPN player projections """
        headers = self.default_headers
        return self.get_json(self.api_url, headers=headers, params=self.default_params)


class Parser:
    """
    Parse ESPN API for football stats

    """

    POSITION_MAP = {
        1: "QB",
        2: "RB",
        3: "WR",
        4: "TE",
        5: "K",
        16: "DST",
    }

    STAT_MAP = {
        "0": "pass_att",
        "1": "pass_cmp",
        "3": "pass_yds",
        "4": "pass_td",
        "19": "pass_tpc",
        "20": "pass_int",
        "23": "rush_att",
        "24": "rush_yds",
        "25": "rush_td",
        "26": "rush_tpc",
        "53": "rec_rec",
        "42": "rec_yds",
        "43": "rec_td",
        "44": "rec_tpc",
        "58": "rec_tar",
        "72": "fum_lost",
        "74": "madeFieldGoalsFrom50Plus",
        "77": "madeFieldGoalsFrom40To49",
        "80": "madeFieldGoalsFromUnder40",
        "85": "missedFieldGoals",
        "86": "madeExtraPoints",
        "88": "missedExtraPoints",
        "89": "defensive0PointsAllowed",
        "90": "defensive1To6PointsAllowed",
        "91": "defensive7To13PointsAllowed",
        "92": "defensive14To17PointsAllowed",
        "93": "defensiveBlockedKickForTouchdowns",
        "95": "defensiveInterceptions",
        "96": "defensiveFumbles",
        "97": "defensiveBlockedKicks",
        "98": "defensiveSafeties",
        "99": "defensiveSacks",
        "101": "kickoffReturnTouchdown",
        "102": "puntReturnTouchdown",
        "103": "fumbleReturnTouchdown",
        "104": "interceptionReturnTouchdown",
        "123": "defensive28To34PointsAllowed",
        "124": "defensive35To45PointsAllowed",
        "129": "defensive100To199YardsAllowed",
        "130": "defensive200To299YardsAllowed",
        "132": "defensive350To399YardsAllowed",
        "133": "defensive400To449YardsAllowed",
        "134": "defensive450To499YardsAllowed",
        "135": "defensive500To549YardsAllowed",
        "136": "defensiveOver550YardsAllowed",
    }

    TEAM_MAP = {
        "ARI": 22,
        "ATL": 1,
        "BAL": 33,
        "BUF": 2,
        "CAR": 29,
        "CHI": 3,
        "CIN": 4,
        "CLE": 5,
        "DAL": 6,
        "DEN": 7,
        "DET": 8,
        "GB": 9,
        "HOU": 34,
        "IND": 11,
        "JAC": 30,
        "JAX": 30,
        "KC": 12,
        "LAC": 24,
        "LA": 14,
        "LAR": 14,
        "MIA": 15,
        "MIN": 16,
        "NE": 17,
        "NO": 18,
        "NYG": 19,
        "NYJ": 20,
        "OAK": 13,
        "PHI": 21,
        "PIT": 23,
        "SEA": 26,
        "SF": 25,
        "TB": 27,
        "TEN": 10,
        "WAS": 28,
        "WSH": 28,
        "FA": 0,
    }

    TEAM_CODES = {
        'ARI': ['ARI', 'Arizona Cardinals', 'Cardinals', 'Arizona', 'crd'],
        'ATL': ['ATL', 'Atlanta Falcons', 'Falcons', 'Atlanta', 'atl'],
        'BAL': ['BAL', 'Baltimore Ravens', 'Ravens', 'Baltimore', 'rav'],
        'BUF': ['BUF', 'Buffalo Bills', 'Bills', 'Buffalo', 'buf'],
        'CAR': ['CAR', 'Carolina Panthers', 'Panthers', 'Carolina', 'car'],
        'CHI': ['CHI', 'Chicago Bears', 'Bears', 'Chicago', 'chi'],
        'CIN': ['CIN', 'Cincinnati Bengals', 'Bengals', 'Cincinnati', 'cin'],
        'CLE': ['CLE', 'Cleveland Browns', 'Browns', 'Cleveland', 'cle'],
        'DAL': ['DAL', 'Dallas Cowboys', 'Cowboys', 'Dallas', 'dal'],
        'DEN': ['DEN', 'Denver Broncos', 'Broncos', 'Denver', 'den'],
        'DET': ['DET', 'Detroit Lions', 'Lions', 'Detroit', 'det'],
        'GB': ['GB', 'Green Bay Packers', 'Packers', 'Green Bay', 'GNB', 'gnb'],
        'HOU': ['HOU', 'Houston Texans', 'Texans', 'Houston', 'htx'],
        'IND': ['IND', 'Indianapolis Colts', 'Colts', 'Indianapolis', 'clt'],
        'JAC': ['JAC', 'Jacksonville Jaguars', 'Jaguars', 'Jacksonville', 'jac', 'jax'],
        'KC': ['KC', 'Kansas City Chiefs', 'Chiefs', 'Kansas City', 'kan', 'KAN'],
        'LAC': ['LAC', 'Los Angeles Chargers', 'LA Chargers', 'San Diego Chargers', 'Chargers', 'San Diego', 'SD', 'sdg', 'SDG'],
        'LAR': ['LAR', 'LA', 'Los Angeles Rams', 'LA Rams', 'St. Louis Rams', 'Rams', 'St. Louis', 'ram'],
        'MIA': ['MIA', 'Miami Dolphins', 'Dolphins', 'Miami', 'mia'],
        'MIN': ['MIN', 'Minnesota Vikings', 'Vikings', 'Minnesota', 'min'],
        'NE': ['NE', 'New England Patriots', 'Patriots', 'New England', 'NEP', 'nwe', 'NWE'],
        'NO': ['NO', 'New Orleans Saints', 'Saints', 'New Orleans', 'NOS', 'nor', 'NOR'],
        'NYG': ['NYG', 'New York Giants', 'Giants', 'nyg'],
        'NYJ': ['NYJ', 'New York Jets', 'Jets', 'nyj'],
        'OAK': ['OAK', 'Oakland Raiders', 'Raiders', 'Oakland', 'rai'],
        'PHI': ['PHI', 'Philadelphia Eagles', 'Eagles', 'Philadelphia', 'phi'],
        'PIT': ['PIT', 'Pittsburgh Steelers', 'Steelers', 'Pittsburgh', 'pit'],
        'SF': ['SF', 'San Francisco 49ers', '49ers', 'SFO', 'San Francisco', 'sfo'],
        'SEA': ['SEA', 'Seattle Seahawks', 'Seahawks', 'Seattle', 'sea'],
        'TB': ['TB', 'Tampa Bay Buccaneers', 'Buccaneers', 'TBO', 'tam', 'TAM', 'Tampa', 'Tampa Bay'],
        'TEN': ['TEN', 'Tennessee Titans', 'Titans', 'Tennessee', 'oti'],
        'WAS': ['WAS', 'Washington Redskins', 'Redskins', 'Washington', 'was']
    }

    def __init__(self, season, week):
        """
            """
        logging.getLogger(__name__).addHandler(logging.NullHandler())
        self.season = season
        self.week = week

    def _find_projection(self, stats):
        """Simplified way to find projection or result"""
        mapping = {
            "seasonId": self.season,
            "scoringPeriodId": self.week,
            "statSourceId": 1,
            "statSplitTypeId": 0
        }

        for item in stats:
            if {k: item[k] for k in mapping} == mapping:
                return item

    def _get_team_code(self, team):
        """Standardizes team code across sites

        Args:
            team (str): the code or team name

        Returns:
            str: 2-3 letter team code, ATL, BAL, etc.

        Examples:
            >>>team_code('Ravens')
            'BAL'

            >>>team_code('JAC')
            'JAX'
        """
        if team in self.TEAM_CODES:
            return team
        matches = [(k, v) for k, v in self.TEAM_CODES.items()
                if (team in v or
                    team.title() in v or
                    team.lower() in v or
                    team.upper() in v)
                ]
        if len(matches) == 1:
            return matches[0][0]
        raise ValueError(f'no match for {team}')

    def _parse_stats(self, stat):
        """Parses stats dict"""
        return {
            self.STAT_MAP.get(str(k)): float(v)
            for k, v in stat.items()
            if str(k) in self.STAT_MAP
        }

    def espn_team(self, team_code=None, team_id=None):
        """Returns team_id given code or team_code given team_id"""
        if team_code:
            tid = self.TEAM_MAP.get(team_code)
            return tid if tid else self.TEAM_MAP.get(self._get_team_code(team_code))
        elif team_id:
            return {v: k for k, v in self.TEAM_MAP.items()}.get(int(team_id))

    def projections(self, content):
        """Parses the seasonal projections
        
        Args:
            content(dict): parsed JSON
            week(int): 1-17

        Returns:
            list: of dict
        """
        proj = []

        top_level_keys = {
            "id": "source_player_id",
            "fullName": "source_player_name",
            "proTeamId": "source_team_id",
        }

        for player in [item["player"] for item in content["players"]]:
            p = {
                top_level_keys.get(k): v
                for k, v in player.items()
                if k in top_level_keys
            }

            p["source_team_code"] = self.espn_team(team_id=p.get("source_team_id", 0))

            # loop through player stats to find projections
            stat = self._find_projection(player["stats"])
            if stat:
                p["source_player_projection"] = stat["appliedTotal"]
                proj.append(dict(**p, **self._parse_stats(stat["stats"])))
            else:
                p["source_player_projection"] = None
                proj.append(p)
        return proj

    def weekly_projections(self, content, week):
        """Parses the weekly projections

        Args:
            content(dict): parsed JSON
            week(int): 1-17

        Returns:
            list: of dict
        """
        proj = []

        top_level_keys = {
            "id": "source_player_id",
            "fullName": "source_player_name",
            "proTeamId": "source_team_id",
            "defaultPositionId": "source_player_position"
        }

        for player in [item["player"] for item in content["players"]]:
            p = {
                top_level_keys.get(k): v
                for k, v in player.items()
                if k in top_level_keys
            }

            p["source_team_code"] = self.espn_team(team_id=p.get("source_team_id", 0))
            p["source_player_position"] = self.POSITION_MAP.get(p["source_player_position"], "UNK")

            # loop through player stats to find weekly projections
            stat = self._find_projection(player["stats"], week)
            if stat:
                p["source_player_projection"] = stat["appliedTotal"]
                proj.append(dict(**p, **self._parse_stats(stat["stats"])))
            else:
                p["source_player_projection"] = None
                proj.append(p)
        return proj



if __name__ == "__main__":
    pass
