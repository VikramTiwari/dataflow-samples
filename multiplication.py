"""
sample program to log a message

python multiplication.py
"""
from __future__ import absolute_import

import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions, SetupOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.io import WriteToText

BUCKET_URL = 'gs://ivikramtiwari-dataflow'  # replace with your bucket name
PROJECT_ID = 'ivikramtiwari'  # replace with your project id
JOB_NAME = 'multiplication'


def multiply(value):
    logging.info('Running for', value)
    return value * value


def run(argv=None):
    """Main entry point"""
    pipeline_options = PipelineOptions()
    pipeline_options.view_as(SetupOptions).save_main_session = True
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.project = PROJECT_ID
    google_cloud_options.job_name = JOB_NAME
    google_cloud_options.staging_location = '%s/staging' % BUCKET_URL
    google_cloud_options.temp_location = '%s/tmp' % BUCKET_URL

    pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'
    p = beam.Pipeline(options=pipeline_options)

    init = p | 'init' >> beam.Create([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]) | 'multiply' >> beam.Map(multiply) | 'output' >> WriteToText(BUCKET_URL + '/results/multiplication')

    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()