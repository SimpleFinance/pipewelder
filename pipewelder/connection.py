# -*- coding: utf-8 -*-
"""
A patch to the boto DataPipelineConnection object.

As of boto 2.36.0, putting and validating pipeline parameters/values
was not supported.
"""

import json

from boto.datapipeline.layer1 import DataPipelineConnection


def put_pipeline_definition(self,
                            pipeline_objects,
                            pipeline_id,
                            parameter_objects=None,
                            parameter_values=None):
    """
    Adds tasks, schedules, and preconditions that control the
    behavior of the pipeline. You can use PutPipelineDefinition to
    populate a new pipeline or to update an existing pipeline that
    has not yet been activated.
    """
    params = {
        'pipelineId': pipeline_id,
        'pipelineObjects': pipeline_objects,
    }
    if parameter_objects is not None:
        params['parameterObjects'] = parameter_objects
    if parameter_values is not None:
        params['parameterValues'] = parameter_values
    return self.make_request(action='PutPipelineDefinition',
                             body=json.dumps(params))


def validate_pipeline_definition(self,
                                 pipeline_objects,
                                 pipeline_id,
                                 parameter_objects=None,
                                 parameter_values=None):
    """
    Tests the pipeline definition with a set of validation checks
    to ensure that it is well formed and can run without error.
    """
    params = {
        'pipelineId': pipeline_id,
        'pipelineObjects': pipeline_objects,
    }
    if parameter_objects is not None:
        params['parameterObjects'] = parameter_objects
    if parameter_values is not None:
        params['parameterValues'] = parameter_values
    return self.make_request(action='ValidatePipelineDefinition',
                             body=json.dumps(params))

DataPipelineConnection.put_pipeline_definition = (
    put_pipeline_definition)
DataPipelineConnection.validate_pipeline_definition = (
    validate_pipeline_definition)
