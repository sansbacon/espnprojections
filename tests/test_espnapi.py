# -*- coding: utf-8 -*-
from functools import lru_cache
import json

import pandas as pd
import pytest
import requests_mock

from espnprojections.espnapi import Scraper, Parser, ESPNProjections


SEASON = 2021

@lru_cache
@pytest.fixture
def content(test_directory):
    return json.loads((test_directory / 'espn.json').read_text())


def test_scraper():
    assert Scraper(SEASON)


def test_parser():
    assert Parser(SEASON, 0)


@requests_mock.Mocker(kw='mock')
def test_season_projections(test_directory, tprint, **kwargs):
    kwargs['mock'].get(requests_mock.ANY, text=(test_directory / 'espn.json').read_text())
    s = Scraper(SEASON)
    p = Parser(season=SEASON, week=0)
    proj = p.projections(s.projections())
    assert isinstance(proj, list)
    assert isinstance(proj[0], dict)


def test_espn_projections_source(test_directory):
    """Tests ESPNProjections"""
    ep = ESPNProjections(rawdir=test_directory, procdir=test_directory)
    assert ep is not None


@requests_mock.Mocker(kw='mock')
def test_espn_projections_source_load_raw(test_directory, tprint, **kwargs):
    """Tests ESPNProjections"""
    kwargs['mock'].get(requests_mock.ANY, text=(test_directory / 'espn.json').read_text())
    ep = ESPNProjections(rawdir=test_directory, procdir=test_directory)
    proj = ep.load_raw(season=SEASON, week=0)
    assert isinstance(proj, pd.DataFrame)
    tprint(proj.columns)


@requests_mock.Mocker(kw='mock')
def test_espn_projections_source_process_raw(test_directory, tprint, **kwargs):
    """Tests ESPNProjections process raw"""
    kwargs['mock'].get(requests_mock.ANY, text=(test_directory / 'espn.json').read_text())
    ep = ESPNProjections(rawdir=test_directory, procdir=test_directory)
    proj = ep.load_raw(season=SEASON, week=0)
    df = ep.process_raw(proj)
    assert isinstance(df, pd.DataFrame)
    assert 'plyr' in df.columns


@requests_mock.Mocker(kw='mock')
def test_espn_projections_source_standardize(test_directory, tprint, **kwargs):
    """Tests ESPNProjections standardize"""
    kwargs['mock'].get(requests_mock.ANY, text=(test_directory / 'espn.json').read_text())
    ep = ESPNProjections(rawdir=test_directory, procdir=test_directory)
    proj = ep.load_raw(season=SEASON, week=0)
    df = ep.process_raw(proj)
    df = ep.standardize(df)
    assert isinstance(df, pd.DataFrame)
    assert 'plyr' in df.columns
