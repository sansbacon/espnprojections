# -*- coding: utf-8 -*-
from functools import lru_cache
import json
import random

import pandas as pd
import pytest
import requests_mock

from espnprojections.watson import Scraper, Parser, WatsonProjections


SEASON = 2021

def test_scraper():
    assert Scraper(SEASON)


def test_parser():
    assert Parser()


@requests_mock.Mocker(kw='mock')
def test_projection(test_directory, tprint, **kwargs):
    kwargs['mock'].get(requests_mock.ANY, text=(test_directory / 'projection.json').read_text())
    s = Scraper(SEASON)
    p = Parser()
    proj = p.projection(s.projection(1))
    assert isinstance(proj, dict)
    tprint(proj)


@requests_mock.Mocker(kw='mock')
def test_player(test_directory, tprint, **kwargs):
    kwargs['mock'].get(requests_mock.ANY, text=(test_directory / 'player.json').read_text())
    s = Scraper(SEASON)
    p = Parser()
    data = p.player(s.player(1))
    assert isinstance(data, dict)
    tprint(data)


@requests_mock.Mocker(kw='mock')
def test_players(test_directory, tprint, **kwargs):
    kwargs['mock'].get(requests_mock.ANY, text=(test_directory / 'players.json').read_text())
    s = Scraper(SEASON)
    p = Parser()
    data = p.players(s.players())
    assert isinstance(data, list)
    assert isinstance(random.choice(data), dict)
    tprint(data)


@requests_mock.Mocker(kw='mock')
def test_playertrend(test_directory, tprint, **kwargs):
    kwargs['mock'].get(requests_mock.ANY, text=(test_directory / 'playertrend.json').read_text())
    s = Scraper(SEASON)
    p = Parser()
    data = p.playertrend(s.playertrend(1))
    assert isinstance(data, list)
    assert isinstance(random.choice(data), dict)
    tprint(data)


@requests_mock.Mocker(kw='mock')
def test_performance(test_directory, tprint, **kwargs):
    kwargs['mock'].get(requests_mock.ANY, text=(test_directory / 'performance.json').read_text())
    s = Scraper(SEASON)
    p = Parser()
    data = p.performance(s.performance(1))
    assert isinstance(data, list)
    assert isinstance(random.choice(data), dict)
    tprint(data)
