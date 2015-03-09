# -*- coding: utf-8 -*-
"""
The core Pipewelder API.
"""

from __future__ import print_function

import re
import os
import logging
import hashlib
from copy import deepcopy
from datetime import datetime, timedelta

from pipewelder import translator
from boto import connect_s3
from boto.s3.key import Key as S3Key

from pipewelder import util

import six
if six.PY2:
    from urlparse import urlparse
else:
    from urllib.parse import urlparse

PIPELINE_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S"
PIPELINE_FREQUENCY_RE = re.compile(r'(?P<number>\d+) (?P<unit>\w+s)')
PIPELINE_PARAM_RE = re.compile(r'\#\{(my[a-zA-Z0-9]+)\}')
PIPEWELDER_STUB_PARAMS = {
    'name': "Pipewelder validation stub",
    'unique_id': 'stub',
    "description": """
This pipeline should always be in 'PENDING' status.
It is used by Pipewelder to validate pipeline definitions.
    """.strip()
}


class Pipewelder(object):
    """
    A collection of Pipelines sharing a definition template.
    """
    def __init__(self, conn, template_path, s3_conn=None):
        """
        *conn* is a :class:`boto.datapipeline.layer1.DataPipelineConnection`
        instance used to manipulate added pipelines,
        *s3_conn* is a :class:`boto.s3.connection.S3Connection`
        used to upload pipeline tasks to S3,
        and *template_path* is the path to a local file containing the
        template pipeline definition.
        """
        self.conn = conn
        self.s3_conn = s3_conn
        if self.s3_conn is None:
            self.s3_conn = connect_s3()
        template_path = os.path.normpath(template_path)
        self.template = definition_from_file(template_path)
        self.pipelines = {}

    def add_pipeline(self, dirpath):
        """
        Load a new :class:`Pipeline` object based on the files contained in
        *dirpath*.
        """
        pipeline = Pipeline(self.conn, self.s3_conn, self.template, dirpath)
        self.pipelines[pipeline.name] = pipeline
        return pipeline

    def are_pipelines_valid(self):
        """
        Returns ``True`` if all pipeline definition validate with AWS.
        """
        return all([p.is_valid() for p in self.pipelines.values()])

    def validate(self):
        """
        Synonym for :meth:`are_pipelines_valid`.
        """
        return self.are_pipelines_valid()

    def upload(self):
        """
        Upload files to S3 corresponding to each pipeline and its tasks.

        Returns ``True`` is successful.
        """
        return all([p.upload() for p in self.pipelines.values()])

    def delete(self):
        """
        Delete all pipeline definitions.

        Returns ``True`` if successful.
        """
        return all([p.delete() for p in self.pipelines.values()])

    def put_definition(self):
        """
        Puts definitions for all pipelines.

        Returns ``True`` if successful.
        """
        return all([p.put_definition() for p in self.pipelines.values()])

    def activate(self):
        """
        Activate all pipeline definitions,
        deleting existing pipeline if needed.

        Returns ``True`` if successful.
        """
        if not self.are_pipelines_valid():
            logging.error("Not activating pipelines due to validation errors.")
            return False
        return all([p.activate() for p in self.pipelines.values()])


class Pipeline(object):
    """
    A class defining a single pipeline definition and associated tasks.
    """
    def __init__(self, conn, s3_conn, template, dirpath):
        """
        Create a Pipeline based on definition dict *template*.

        *dirpath* is a directory containing a 'values.json' file,
        a 'run' executable, and a 'tasks' directory.
        *conn* is a DataPipelineConnection and *s3_conn* is an S3Connection.
        """
        self.conn = conn
        self.s3_conn = s3_conn
        self.dirpath = os.path.normpath(dirpath)
        self.definition = template.copy()
        values_path = os.path.join(dirpath, 'values.json')
        decoded = util.load_json(values_path)
        self.values = decoded.get('values', {})
        if 'myName' not in self.values:
            self.values['myName'] = os.path.basename(dirpath)
        # adjust the start timestamp to the future
        timestamp = self.values['myStartDateTime']
        period = self.values['mySchedulePeriod']
        adjusted_timestamp = adjusted_to_future(timestamp, period)
        self.values['myStartDateTime'] = adjusted_timestamp

    @property
    def name(self):
        return self._get_value('myName')

    @property
    def description(self):
        try:
            return self._get_value('myDescription')
        except ValueError:
            return None

    @property
    def tags(self):
        if 'myTags' not in self.values:
            return {}
        return dict(tag_expression.split(':')
                    for tag_expression in self.values['myTags'])

    @property
    def unique_id(self):
        return hashlib.md5(self.name + str(self.tags)).hexdigest()

    def api_objects(self):
        """
        Return a dict containing the pipeline objects in AWS API format.
        """
        d = deepcopy(self.definition)
        return translator.definition_to_api_objects(d)

    def api_parameters(self):
        """
        Return a dict containing the pipeline parameters in AWS API format.
        """
        d = deepcopy(self.definition)
        return translator.definition_to_api_parameters(d)

    def api_values(self):
        """
        Return a dict containing the pipeline param values in AWS API format.
        """
        d = {'values': self.values}
        return translator.definition_to_parameter_values(d)

    def api_tags(self):
        """
        Return a list containing the pipeline tags in AWS API format.
        """
        tag_list = [{'key': k, 'value': v}
                    for k, v in self.tags.items()]
        return tag_list

    def create(self):
        """
        Create a pipeline in AWS if it does not already exist.

        Returns the pipeline id.
        """
        response = self.conn.create_pipeline(self.name, self.unique_id,
                                             self.description, self.api_tags())
        return response['pipelineId']

    def is_valid(self):
        """
        Returns ``True`` if the pipeline definition validates to AWS.
        """
        response = self.conn.create_pipeline(**PIPEWELDER_STUB_PARAMS)
        pipeline_id = response["pipelineId"]
        response = self.conn.validate_pipeline_definition(
            self.api_objects(), pipeline_id,
            self.api_parameters(), self.api_values())
        self._log_validation_messages(response)
        if response['errored']:
            return False
        else:
            logging.info("Pipeline '{0}' is valid".format(self.name))
            return True

    def upload(self):
        """
        Uploads the contents of `dirpath` to S3.

        The destination path in S3 is determined by 'myS3InputDirectory'
        in the 'values.json' file for this pipeline.
        Existing contents of the 'tasks' subdirectory are deleted.

        Returns ``True`` if successful.
        """
        s3_dir = self._get_value('myS3InputDir')
        bucket_path, input_dir = bucket_and_path(s3_dir)
        bucket = self.s3_conn.get_bucket(bucket_path)

        remote_task_path = os.path.join(input_dir, 'tasks')
        existing_task_keys = bucket.list(prefix=remote_task_path)
        existing_tasks = [key.name for key in existing_task_keys]
        bucket.delete_keys(existing_tasks)
        logging.info("Deleted from bucket '{0}': {1}"
                     .format(bucket_path, existing_tasks))

        with util.cd(self.dirpath):
            for root, dirs, files in os.walk('.'):
                for f in files:
                    filepath = os.path.join(root, f)
                    k = S3Key(bucket)
                    k.key = os.path.normpath(os.path.join(input_dir, filepath))
                    k.set_contents_from_filename(filepath)
                    logging.info('Copied {0} to {1}'
                                 .format(os.path.abspath(filepath),
                                         os.path.normpath(
                                             os.path.join(s3_dir, filepath))))
        return True

    def delete(self):
        """
        Delete this pipeline definition from AWS.

        Returns ``True`` if successful.
        """
        pipeline_id = self.create()
        logging.info("Deleting pipeline with id {0}".format(pipeline_id))
        self.conn.delete_pipeline(pipeline_id)
        return True

    def put_definition(self):
        """
        Put this pipeline definition to AWS.

        Returns ``True`` if successful.
        """
        pipeline_id = self.create()
        logging.info("Putting pipeline definition for {0}".format(pipeline_id))
        self.conn.put_pipeline_definition(self.api_objects(),
                                          pipeline_id,
                                          self.api_parameters(),
                                          self.api_values())
        return True

    def activate(self):
        """
        Activate this pipeline definition in AWS.

        Deletes the existing pipeline if it has previously been activated.

        Returns ``True`` if successful.
        """
        pipeline_id = self.create()
        existing_definition = definition_from_id(self.conn, pipeline_id)
        state = state_from_id(self.conn, pipeline_id)
        if existing_definition == self.definition:
            return True
        elif state == 'PENDING':
            self.put_definition()
        else:
            self.delete()
            return self.activate()
        logging.info("Activating pipeline with id {0}".format(pipeline_id))
        self.conn.activate_pipeline(pipeline_id)
        return True

    def _log_validation_messages(self, response):
        for container in response['validationWarnings']:
            logging.warning("Warnings in validation response for %s",
                            container['id'])
            for message in container['warnings']:
                logging.warning(message)
        for container in response['validationErrors']:
            logging.error("Errors in validation response for %s",
                          container['id'])
            for message in container['errors']:
                logging.error(message)

    def _get_value(self, key):
        if key in self.values:
            return self._parsed_via_parameters(self.values[key])
        params = self.definition['parameters']
        default = fetch_default(params, key)
        if default is None:
            raise ValueError("No value or default found for '{0}'"
                             .format(key))
        return self._parsed_via_parameters(default)

    def _parsed_via_parameters(self, expression):
        placeholders = re.findall(PIPELINE_PARAM_RE, expression)
        if not placeholders:
            return expression
        key = placeholders[0]
        value = self._get_value(key)
        placeholder = '#{' + key + '}'
        expression = expression.replace(placeholder, value)
        return self._parsed_via_parameters(expression)

    def _parsed_object(self, name):
        return parsed_object(self.conn, self.create(), name)

    def _parsed_location(self, name):
        obj = self._parsed_object(name)
        fetch_field_value(obj, 'directoryPath')


def bucket_and_path(s3_uri):
    """
    Return a bucket name and key path from *s3_uri*.

    >>> bucket_and_path('s3://pipewelder-bucket/pipewelder-test/inputs')
    ('pipewelder-bucket', 'pipewelder-test/inputs')
    """
    uri = urlparse(s3_uri)
    return (uri.netloc, uri.path[1:])


def parse_period(period):
    """
    Return a timedelta object parsed from string *period*.

    >>> parse_period("15 minutes")
    datetime.timedelta(0, 900)
    >>> parse_period("3 hours")
    datetime.timedelta(0, 10800)
    >>> parse_period("1 days")
    datetime.timedelta(1)
    """
    parts = PIPELINE_FREQUENCY_RE.match(period)
    if not parts:
        raise ValueError("'{0}' cannot be parsed as a period".format(period))
    parts = parts.groupdict()
    kwargs = {parts['unit']: int(parts['number'])}
    return timedelta(**kwargs)


def adjusted_to_future(timestamp, period):
    """
    Return *timestamp* string, adjusted to the future if necessary.

    If *timestamp* is in the future, it will be returned unchanged.
    If it's in the past, *period* will be repeatedly added until the
    result is in the future.

    All times are assumed to be in UTC.

    >>> adjusted_to_future('2199-01-01T00:00:00', '1 days')
    '2199-01-01T00:00:00'
    """
    dt = datetime.strptime(timestamp, PIPELINE_DATETIME_FORMAT)
    delta = parse_period(period)
    now = datetime.utcnow()
    while dt < now:
        dt += delta
    return dt.strftime(PIPELINE_DATETIME_FORMAT)


def fetch_field_value(aws_response, field_name):
    """
    Return a value nested within the 'fields' entry of dict *aws_response*.

    The returned value is the second item from a dict with 'key' *field_name*.

    >>> r = {'fields': [{'key': 'someKey', 'stringValue': 'someValue'}]}
    >>> fetch_field_value(r, 'someKey')
    'someValue'
    """
    for container in aws_response['fields']:
        if container['key'] == field_name:
            for (k, v) in container.items():
                if k != 'key':
                    return v
    raise ValueError("Did not find a field called {0} in response {1}"
                     .format(field_name, aws_response))


def fetch_default(params, key):
    """
    Return the default associated with *key* from parameter list *params*.

    If no default, returns None.
    >>> p = [{'type': 'String', 'id': 'myParam', 'default': 'foo'}]
    >>> fetch_default(p, 'myParam')
    'foo'
    >>> p = [{'type': 'String', 'id': 'myParam'}]
    >>> fetch_default(p, 'myParam')
    """
    for container in params:
        if container['id'] == key:
            if 'default' in container:
                return container['default']
    return None


def state_from_id(conn, pipeline_id):
    """
    Return the *@pipelineState* string for object matching *pipeline_id*.

    *conn* is a DataPipelineConnection object.
    """
    response = conn.describe_pipelines([pipeline_id])
    description = response['pipelineDescriptionList'][0]
    return fetch_field_value(description, '@pipelineState')


def definition_from_file(filename):
    """
    Return a dict containing the contents of pipeline definition *filename*.
    """
    return util.load_json(filename)


def definition_from_id(conn, pipeline_id):
    """
    Return a dict containing the definition of *pipeline_id*.

    *conn* is a DataPipelineConnection object.
    """
    response = conn.get_pipeline_definition(pipeline_id)
    return translator.api_to_definition(response)


def parsed_objects(conn, pipeline_id, object_ids):
    """
    Return a list of object dicts as evaluated by Data Pipeline.
    """
    response = conn.describe_objects(object_ids, pipeline_id,
                                     evaluate_expressions=True)
    return response['pipelineObjects']


def parsed_object(conn, pipeline_id, object_id):
    """
    Return an object dict as evaluated by Data Pipeline.
    """
    return parsed_objects(conn, pipeline_id, [object_id])[0]
