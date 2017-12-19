"""
sample program to log a message

python hello_world.py

check step logs for "Log message" step in cloud console
"""
from __future__ import absolute_import

import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions, SetupOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions

BUCKET_URL = 'gs://ivikramtiwari-dataflow'  # replace with your bucket name
PROJECT_ID = 'ivikramtiwari'  # replace with your project id
MESSAGE = 'Hello world!'
JOB_NAME = 'helloworld'


class HelloWorld(beam.DoFn):
    def __init__(self, message):
        super(HelloWorld, self).__init__()
        self._message = message

    def process(self, element):
        # log the message
        logging.info(self._message)


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

    init = p | 'Begin pipeline' >> beam.Create(
        ["test"]) | 'Log message' >> beam.ParDo(HelloWorld(MESSAGE))

    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()