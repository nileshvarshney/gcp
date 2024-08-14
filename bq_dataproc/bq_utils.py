# gcloud dataproc jobs submit pyspark gs://playground-s-11-318d2d4e-dataproc/codes/bq_read.py --cluster=bluesea --region=us-central1 --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar --files="gs://playground-s-11-318d2d4e-dataproc/codes/configs/project_parameters.yaml"

import yaml
from pyspark.sql import SparkSession
from google.cloud import bigquery

cfg_d = dict()
f = open('project_parameters.yaml')
cfg_d = yaml.safe_load(f)
f.close()

bq_project_name = cfg_d['bq_project_name']
staging_location = cfg_d['staging_location']
service_account = cfg_d['service_account']
project_name = cfg_d['project_name']


bq_client = bigquery.Client(project=project_name)

def get_full_table_name(table_name):
    return str(f"{bq_project_name}.{table_name}")


def get_spark_session(app_name):
    spark_session = (
        SparkSession.builder.appName(app_name)
        .config("viewEnabled", "true")
        .config("temporaryGcsBucket",staging_location)
        .getOrCreate()
    )
    return spark_session


def read_table_or_view(spark_session, object_name, column_list=None):
    query = ""
    if column_list is None:
        query = "SELECT * FROM `{}`".format(get_full_table_name(object_name))
    if column_list:
        query = "SELECT {} FROM `{}`".format(','.join(column_list), get_full_table_name(object_name))

    job_config = bigquery.QueryJobConfig()
    query_job = bq_client.query(query=query, job_config=job_config)
    query_job.result()

    bq_cache = ( 
        query_job.destination.to_api_repr()["projectId"]
        + '.'
        + query_job.destination.to_api_repr()["datasetId"]
        + '.'
        + query_job.destination.to_api_repr()["tableId"]
    )

    print("BQ Cache Table: " + bq_cache)
    return (
        spark_session.read.format("bigquery")
        .option("table", bq_cache)
        .load()
    )

    