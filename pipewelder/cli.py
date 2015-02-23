#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
The Pipewelder command-line interface.
"""

from __future__ import print_function

import argparse
import os
import sys
import boto.datapipeline
import json

from glob import glob

from pipewelder import metadata, util, Pipewelder

import logging
logging.basicConfig(level="INFO")


CONFIG_DEFAULTS = {
    "dirs": ["*"],
    "region": "",
    "template": "pipeline_definition.json",
    "values": [],
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
Pipewelder {version}
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
    parser.add_argument(
        'action',
        help="""Action to take:
        'validate' pipeline definitions with AWS;
        'upload' pipeline files to S3;
        'activate' defined pipelines
        """)
    parser.add_argument(
        '--group',
        default=None,
        help="Group within pipewelder.json to act on; defaults to all")

    # subparsers = parser.add_subparsers(dest='parser',
    #                                    help='sub-command help')
    # subparsers.add_parser(
    #     'validate', help="validate pipeline definitions with AWS"
    # ).set_defaults(method='validate')
    # subparsers.add_parser(
    #     'upload', help="upload pipeline files to S3"
    # ).set_defaults(method='upload')
    # subparsers.add_parser(
    #     'activate', help="activate all defined pipelines"
    # ).set_defaults(method='activate')

    args = parser.parse_args(args=argv[1:])

    defaults = {}

    if 'AWS_ACCESS_KEY_ID' not in os.environ:
        parser.error("Must set AWS_ACCESS_KEY_ID")
    if 'AWS_SECRET_ACCESS_KEY' not in os.environ:
        parser.error("Must set AWS_SECRET_ACCESS_KEY")
    if 'AWS_DEFAULT_REGION' in os.environ:
        defaults['region'] = os.environ['AWS_DEFAULT_REGION']

    config_path = os.path.exists('pipewelder.json') and 'pipewelder.json' or None
    configs = pipewelder_configs(config_path, defaults)
    print("Reading configuration from {}".format(config_path))

    for name, config in configs.items():
        if args.group and args.group != name:
            continue
        if name == 'defaults':
            continue
        print("Acting on configuration '{0}'".format(name))
        conn = boto.datapipeline.connect_to_region(config['region'])
        try:
            pw = Pipewelder(conn, config['template'])
        except IOError as e:
            print(e)
            return 1
        for d in config['dirs']:
            p = pw.add_pipeline(d)
            for k, v in config["values"].items():
                p.values[k] = v
            print(p.values)
            return_value = call_method(pw, args.action)
            if not return_value:
                print("Failed '{}' action for {}"
                      .format(args.action, config['name']))
                return 1

    return 0


def entry_point():
    """
    Zero-argument entry point for use with setuptools/distribute.
    """
    raise SystemExit(main(sys.argv))


def pipewelder_configs(filename=None, defaults=None):
    """
    Parse json from *filename* for Pipewelder object configurations.

    Returns a dict which maps config names to dicts of options.
    """
    if filename is None:
        data = {"pipewelder": {}}
        dirname = os.path.abspath('.')
    else:
        dirname = os.path.dirname(os.path.abspath(filename))
        with open(filename) as f:
            data = json.load(f)
    defaults = defaults or {}
    data_defaults = data.get('defaults', {})
    defaults = dict(list(CONFIG_DEFAULTS.items()) +
                    list(data_defaults.items()) +
                    list(defaults.items()))
    outputs = {}
    for name in data:
        if name == 'defaults':
            continue
        this_config = dict(list(defaults.items()) +
                           data[name].items())
        dirs = []
        with util.cd(dirname):
            for entry in this_config['dirs']:
                for item in glob(entry):
                    if os.path.isdir(item):
                        dirs.append(item)
        outputs[name] = {
            "name": name,
            "dirs": dirs,
            "region": this_config['region'],
            "template": this_config['template'],
            "values": this_config['values'],
        }
    return outputs


def call_method(obj, name):
    """
    Call the method *name* on *obj*.
    """
    return getattr(obj, name)()


if __name__ == '__main__':
    entry_point()
