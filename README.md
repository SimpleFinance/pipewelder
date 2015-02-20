# Pipelayer

![A worker welding a pipe](welder.jpg)

Pipelayer is a framework that provides a command-line tool and Python API
to manage [AWS Data Pipeline](http://aws.amazon.com/datapipeline/) jobs from flat files.
Simple uses it as a cron-like job scheduler.

## Overview

Pipelayer aims to ease the task of scheduling jobs by defining very simple
pipelines which are little more than an execution schedule, offloading
most of the execution logic to files in S3.
Pipelayer uses Data Pipeline's concept of [data staging](http://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-concepts-staging.html) to pull input files from S3 at the beginning of execution and to upload output files back to S3 at the end of execution.

If you follow Pipelayer's directory structure, all of your pipeline logic
can live in version-controlled flat files. The included command-line interface
gives you simple commands to validate your pipeline definitions, upload
task definitions to S3, and activate your pipelines.

## Installation

Pipelayer is (will be...) available from PyPI:
```
pip install pipelayer
```

## Directory Structure

To use Pipelayer, you provide a template pipeline definition along with
one or more directories that correspond to particular pipeline instances.
The directory structure looks like this
(see [test_data](tests/test_data) for a working example):
```
pipeline_definition.json
my_first_pipeline/
    run
    values.json
    tasks/
        task1.sh
        task2.sh
my_second_pipeline/
...
```

The `values.json` file in each pipeline directory specifies some metadata
(a name and description for the pipeline) along with parameter values
that can be used to modify the template definition.
Those parameter values include the S3 paths for inputs, outputs, and logs.

A [`ShellCommandActivity`](http://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-object-shellcommandactivity.html) in the template definition simply looks for an executable file named `run` and executes it.
`run` is where you put the main logic for your pipeline.

Often, your `run` executable will be a wrapper script to execute a variety of similar tasks.
When that's the case, use the `tasks` subdirectory to hold these definitions.
These tasks could be text files, shell scripts, SQL code, or whatever else
your `run` file expects.
Pipelayer gives `tasks` folder special treatment in that the CLI will make
sure to remove existing task definitions when uploading files.

## Using the Command-Line Interface

The Pipelayer CLI should always be invoked from the top-level directory
of your definitions (the directory where `pipeline_definition.json` lives).
If your directory structure matches Pipelayer's expectations, it should
work without further configuration.

As you make changes to your template definition or `values.json` files,
it can be useful to check whether AWS considers your definitions valid:
```
$ pipelayer validate
```

Once you've defined your pipelines, you'll need to upload the files to S3:
```
$ pipelayer upload
```

Finally, activate your pipelines:
```
$ pipelayer activate
```

Any time you change the `values.json` or `pipeline_definition.json`, you'll
need to run the `activate` subcommand again. Because active pipelines can't
be modified, the `activate` command will delete the existing pipeline and
create a new one in its place. The run history for the previous pipeline will
be discarded.

## Acknowledgments

Pipelayer's package structure is based on [python-project-template](https://github.com/seanfisk/python-project-template).
