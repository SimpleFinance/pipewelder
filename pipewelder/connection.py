# -*- coding: utf-8 -*-

# The code in this file is modified from:
#   https://github.com/boto/boto/blob/2.36.0/boto/datapipeline/layer1.py
#
# The original code carries the following license:
# # Copyright (c) 2013 Amazon.com, Inc. or its affiliates.  All Rights Reserved
# #
# # Permission is hereby granted, free of charge, to any person obtaining a
# # copy of this software and associated documentation files (the
# # "Software"), to deal in the Software without restriction, including
# # without limitation the rights to use, copy, modify, merge, publish, dis-
# # tribute, sublicense, and/or sell copies of the Software, and to permit
# # persons to whom the Software is furnished to do so, subject to the fol-
# # lowing conditions:
# #
# # The above copyright notice and this permission notice shall be included
# # in all copies or substantial portions of the Software.
# #
# # THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
# # OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABIL-
# # ITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT
# # SHALL THE AUTHOR BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# # WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# # OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
# # IN THE SOFTWARE.

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


def create_pipeline(self, name, unique_id, description=None, tags=None):
    """
    Creates a new empty pipeline. When this action succeeds, you
    can then use the PutPipelineDefinition action to populate the
    pipeline.
    :type name: string
    :param name: The name of the new pipeline. You can use the same name
        for multiple pipelines associated with your AWS account, because
        AWS Data Pipeline assigns each new pipeline a unique pipeline
        identifier.
    :type unique_id: string
    :param unique_id: A unique identifier that you specify. This identifier
        is not the same as the pipeline identifier assigned by AWS Data
        Pipeline. You are responsible for defining the format and ensuring
        the uniqueness of this identifier. You use this parameter to ensure
        idempotency during repeated calls to CreatePipeline. For example,
        if the first call to CreatePipeline does not return a clear
        success, you can pass in the same unique identifier and pipeline
        name combination on a subsequent call to CreatePipeline.
        CreatePipeline ensures that if a pipeline already exists with the
        same name and unique identifier, a new pipeline will not be
        created. Instead, you'll receive the pipeline identifier from the
        previous attempt. The uniqueness of the name and unique identifier
        combination is scoped to the AWS account or IAM user credentials.
    :type description: string
    :param description: The description of the new pipeline.
    """
    params = {
        'name': name,
        'uniqueId': unique_id,
    }
    if description is not None:
        params['description'] = description
    if tags is not None:
        params['tags'] = tags
    return self.make_request(action='CreatePipeline',
                             body=json.dumps(params))


DataPipelineConnection.put_pipeline_definition = (
    put_pipeline_definition)
DataPipelineConnection.validate_pipeline_definition = (
    validate_pipeline_definition)
DataPipelineConnection.create_pipeline = (
    create_pipeline)
