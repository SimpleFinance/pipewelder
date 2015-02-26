# -*- coding: utf-8 -*-
from pytest import raises

# The parametrize function is generated, so this doesn't work:
#
#     from pytest.mark import parametrize
#
import pytest
parametrize = pytest.mark.parametrize  # NOPEP8

import os

from pipewelder.cli import pipewelder_configs, main, metadata

import logging
logging.basicConfig(level=logging.INFO)

HERE = os.path.abspath(os.path.dirname(__file__))
DATA_DIR = os.path.join(HERE, 'test_data')


def data_path(path):
    return os.path.join(DATA_DIR, path)


def test_pipewelder_configs():
    configs = pipewelder_configs(data_path('pipewelder.json'))
    assert configs["dev"] == {
        "name": "dev",
        "dirs": ["echoer"],
        "region": "us-west-2",
        "template": "pipeline_definition.json",
        "values": {
            "myEnv": "dev"
        }
    }


class TestMain(object):
    @parametrize('helparg', ['-h', '--help'])
    def test_help(self, helparg, capsys):
        with raises(SystemExit) as exc_info:
            main(['progname', helparg])
        out, err = capsys.readouterr()
        # Should have printed some sort of usage message. We don't
        # need to explicitly test the content of the message.
        assert 'usage' in out
        # Should have used the program name from the argument
        # vector.
        assert 'progname' in out
        # Should exit with zero return code.
        assert exc_info.value.code == 0

    @parametrize('versionarg', ['-V', '--version'])
    def test_version(self, versionarg, capsys):
        with raises(SystemExit) as exc_info:
            main(['progname', versionarg])
        out, err = capsys.readouterr()
        # Should print out version.
        expected = '{0} {1}\n'.format(metadata.project, metadata.version)
        assert (out == expected or err == expected)
        # Should exit with zero return code.
        assert exc_info.value.code == 0
