"""
exports bigquery table to google storage

python export_bigquery_table_to_google_storage.py
"""
from __future__ import absolute_import

import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions, SetupOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from google.cloud import bigquery
from google.cloud.bigquery import Dataset
import uuid
import time

BUCKET_URL = 'gs://ivikramtiwari-dataflow'  # replace with your bucket name
PROJECT_ID = 'ivikramtiwari'  # replace with your project id
MESSAGE = 'Hello world!'
JOB_NAME = 'exporttable'

dataset_id = 'sample' # replace with your dataset id
table_id = 'helloworld' # replace with your table id
export_bucket_url = 'gs://ivikramtiwari-code/data/sample/helloworld.csv' # replace with your bucket url


class ExportTable(beam.DoFn):
    def __init__(self, dataset_id, table_id, export_bucket_url):
        super(ExportTable, self).__init__()
        self._dataset_id = dataset_id
        self._table_id = table_id
        self._export_bucket_url = export_bucket_url

    def process(self, element):
        # exports the file to google storage
        client = bigquery.Client()
        table = client.dataset(self._dataset_id).table(self._table_id)
        job_name = str(uuid.uuid4())

        job = client.extract_table_to_storage(job_name, table, self._export_bucket_url)
        job.begin()
        wait_for_job(job)
        # table.extract(destination=self._export_bucket_url)
        logging.info('extraction successful')


def wait_for_job(job):
    while True:
        job.reload()
        if job.state == 'DONE':
            if job.error_result:
                raise RuntimeError(job.errors)
            return
        time.sleep(1)

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
        ["test"]) | 'Export table' >> beam.ParDo(
            ExportTable(dataset_id, table_id, export_bucket_url))

    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()