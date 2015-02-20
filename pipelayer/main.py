#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
The Pipelayer command-line interface.
"""

from __future__ import print_function

import argparse
import os
import sys
import boto.datapipeline

from six.moves import configparser
from glob import glob

import logging
logging.basicConfig(level="INFO")

from pipelayer import metadata, util, Pipelayer


CONFIG_DEFAULTS = {
    "dirs": "*",
    "region": "",
    "template": "pipeline_definition.json",
}


def main(argv):
    """Program entry point.
    :param argv: command-line arguments
    :type argv: :class:`list`
    """
    author_strings = []
    for name, email in zip(metadata.authors, metadata.emails):
        author_strings.append('Author: {0} <{1}>'.format(name, email))

    epilog = '''
Pipelayer {version}
{authors}
URL: <{url}>
'''.format(
        project=metadata.project,
        version=metadata.version,
        authors='\n'.join(author_strings),
        url=metadata.url)

    parser = argparse.ArgumentParser(
        prog=argv[0],
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=metadata.description,
        epilog=epilog)
    parser.add_argument(
        '-V', '--version',
        action='version',
        version='{0} {1}'.format(metadata.project, metadata.version))

    subparsers = parser.add_subparsers(dest='parser',
                                       help='sub-command help')
    subparsers.add_parser(
        'validate', help="validate pipeline definitions with AWS"
    ).set_defaults(method='validate')
    subparsers.add_parser(
        'upload', help="upload pipeline files to S3"
    ).set_defaults(method='upload')
    subparsers.add_parser(
        'activate', help="activate all defined pipelines"
    ).set_defaults(method='activate')

    args = parser.parse_args(args=argv[1:])

    defaults = {}

    if 'AWS_ACCESS_KEY_ID' not in os.environ:
        parser.error("Must set AWS_ACCESS_KEY_ID")
    if 'AWS_SECRET_ACCESS_KEY' not in os.environ:
        parser.error("Must set AWS_SECRET_ACCESS_KEY")
    if 'AWS_DEFAULT_REGION' in os.environ:
        defaults['region'] = os.environ['AWS_DEFAULT_REGION']

    config_path = os.path.exists('pipelayer.cfg') and 'pipelayer.cfg' or None
    configs = pipelayer_configs(config_path, defaults)
    print("Reading configuration from {}".format(config_path))

    for config in configs:
        conn = boto.datapipeline.connect_to_region(config['region'])
        pl = Pipelayer(conn, config['template'])
        for d in config['dirs']:
            pl.add_pipeline(d)
            return_value = call_method(pl, args.method)
            if not return_value:
                print("Failed '{}' action for {}"
                      .format(args.method, config['name']))
                return 1

    return 0


def entry_point():
    """
    Zero-argument entry point for use with setuptools/distribute.
    """
    raise SystemExit(main(sys.argv))


def pipelayer_configs(filename=None, defaults=None):
    """

    """
    dirname = os.path.dirname(os.path.abspath(filename))
    defaults = defaults or {}
    defaults = dict(CONFIG_DEFAULTS.items() + defaults.items())
    config = configparser.SafeConfigParser(defaults)
    if filename is not None and not os.path.exists(filename):
        raise IOError("No file found at '{}'".format(filename))
    config.read(filename)
    outputs = []
    if not config.sections():
        config.add_section('Pipelayer')
    for section in config.sections():
        dirs = []
        dir_entries = config.get(section, 'dirs').split(' ')
        with util.cd(dirname):
            for entry in dir_entries:
                for item in glob(entry):
                    if os.path.isdir(item):
                        dirs.append(item)
        outputs.append({
            "name": section,
            "dirs": dirs,
            "region": config.get(section, 'region'),
            "template": config.get(section, 'template'),
        })
    return outputs


def call_method(obj, name):
    """
    Call the method *name* on *obj*.
    """
    return getattr(obj, name)()


if __name__ == '__main__':
    entry_point()
