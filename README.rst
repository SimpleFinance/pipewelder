Pipewelder
==========

.. figure:: welder.jpg
   :alt: A worker welding a pipe

Pipewelder is a framework that provides a command-line tool and Python
API to manage `AWS Data
Pipeline <http://aws.amazon.com/datapipeline/>`__ jobs from flat files.
Simple uses it as a cron-like job scheduler.

Source
  https://github.com/SimpleFinance/pipewelder

Documentation
  http://pipewelder.readthedocs.org

PyPI
  https://pypi.python.org/pypi/pipewelder

Overview
--------

Pipewelder aims to ease the task of scheduling jobs by defining very
simple pipelines which are little more than an execution schedule,
offloading most of the execution logic to files in S3. Pipewelder uses
Data Pipeline's concept of `data
staging <http://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-concepts-staging.html>`__
to pull input files from S3 at the beginning of execution and to upload
output files back to S3 at the end of execution.

If you follow Pipewelder's directory structure, all of your pipeline
logic can live in version-controlled flat files. The included
command-line interface gives you simple commands to validate your
pipeline definitions, upload task definitions to S3, and activate your
pipelines.

Installation
------------

Pipewelder is available from `PyPI <https://pypi.python.org/pypi>`__ via
``pip`` and is compatible with Python 2.6, 2.7, 3.3, and 3.4:

::

    pip install pipewelder

The easiest way to get started is to clone the project from GitHub, copy
the example project from Pipewelder's tests, and then modify to suit:

.. code:: bash

    git clone https://github.com/SimpleFinance/pipewelder.git
    cp -r pipewelder/tests/test_data my-pipewelder-project

If you're setting up Pipewelder and need help, feel free to email the
author.

Development
-----------

To do development on Pipewelder, clone the repository and run ``make``
to install dependencies and run tests.

Directory Structure
-------------------

To use Pipewelder, you provide a template pipeline definition along with
one or more directories that correspond to particular pipeline
instances. The directory structure looks like this (see
`test\_data <tests/test_data>`__ for a working example):

::

    pipeline_definition.json
    pipewelder.json <- optional configuration file
    my_first_pipeline/
        run
        values.json
        tasks/
            task1.sh
            task2.sh
    my_second_pipeline/
    ...

The ``values.json`` file in each pipeline directory specifies parameter
values that are used modify the template definition including the S3
paths for inputs, outputs, and logs. Some of these values are used
directly by Pipewelder as well.

A
```ShellCommandActivity`` <http://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-object-shellcommandactivity.html>`__
in the template definition simply looks for an executable file named
``run`` and executes it. ``run`` is the entry point for whatever work
you want your pipeline to do.

Often, your ``run`` executable will be a wrapper script to execute a
variety of similar tasks. When that's the case, use the ``tasks``
subdirectory to hold these definitions. These tasks could be text files,
shell scripts, SQL code, or whatever else your ``run`` file expects.
Pipewelder gives ``tasks`` folder special treatment in that the CLI will
make sure to remove existing task definitions when uploading files.

Using the Command-Line Interface
--------------------------------

The Pipewelder CLI should always be invoked from the top-level directory
of your definitions (the directory where ``pipeline_definition.json``
lives). If your directory structure matches Pipewelder's expectations,
it should work without further configuration.

As you make changes to your template definition or ``values.json``
files, it can be useful to check whether AWS considers your definitions
valid:

::

    $ pipewelder validate

Once you've defined your pipelines, you'll need to upload the files to
S3:

::

    $ pipewelder upload

Finally, activate your pipelines:

::

    $ pipewelder activate

Any time you change the ``values.json`` or ``pipeline_definition.json``,
you'll need to run the ``activate`` subcommand again. Because active
pipelines can't be modified, the ``activate`` command will delete the
existing pipeline and create a new one in its place. The run history for
the previous pipeline will be discarded.

Acknowledgments
---------------

Pipewelder's package structure is based on
`python-project-template <https://github.com/seanfisk/python-project-template>`__.
