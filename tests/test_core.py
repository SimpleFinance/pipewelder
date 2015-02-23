# -*- coding: utf-8 -*-

import pytest
import os

from pipewelder import core
from datetime import datetime

import logging
logging.basicConfig(level=logging.INFO)

HERE = os.path.abspath(os.path.dirname(__file__))
DATA_DIR = os.path.join(HERE, 'test_data')


def data_path(path):
    return os.path.join(DATA_DIR, path)


def test_adjusted_to_future():
    now = datetime.utcnow()
    timestamp = "{0}-01-01T00:00:00".format(now.year)
    adjusted = core.adjusted_to_future(timestamp, "1 days")
    target_dt = datetime(year=now.year, month=now.month, day=(now.day + 1))
    assert adjusted == target_dt.strftime(core.PIPELINE_DATETIME_FORMAT)


@pytest.fixture
def pipeline_description():
    return {
        u'description': u'my description',
        u'fields': [
            {u'key': u'@pipelineState', u'stringValue': u'PENDING'},
            {u'key': u'@creationTime', u'stringValue': u'2015-02-11T21:17:10'},
            {u'key': u'@sphere', u'stringValue': u'PIPELINE'},
            {u'key': u'uniqueId', u'stringValue': u'pipeweldertest1'},
            {u'key': u'@accountId', u'stringValue': u'543715240000'},
            {u'key': u'description', u'stringValue': u'my description'},
            {u'key': u'name', u'stringValue': u'Pipewelder test'},
            {u'key': u'pipelineCreator', u'stringValue': u'AIDAIWZQRURDOOOOO'},
            {u'key': u'@id', u'stringValue': u'df-07437251YGRXOY19OOOO'},
            {u'key': u'@userId', u'stringValue': u'AIDAIWZQRURDXI4UKOOOO'}],
        u'name': u'Pipewelder test',
        u'pipelineId': u'df-07437251YGRXOY19OOOO',
        u'tags': [],
    }


def test_pipeline_state(pipeline_description):
    state = core.fetch_field_value(pipeline_description, '@pipelineState')
    assert state == 'PENDING'
