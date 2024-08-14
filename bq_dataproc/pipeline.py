# gcloud dataproc jobs submit pyspark pipeline.py --cluster=bluesea --region=us-central1 --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructField, StructType, StringType, IntegerType

spark = SparkSession.builder.appName('bq_dataproc').config('spark.jars', 'gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar').getOrCreate()

spark.conf.set("spark.sql.repl.eagerEval.enabled",True)

GCP_PROJECT_NAME = 'playground-s-11-318d2d4e'
BIGQUERY_DATESET_NAME = 'students'
TEMPORARY_BUCKET_NAME = 'dateproc_temp'


def store_df_as_table(dataframe: DataFrame, dataset_name, table_name):
    dataframe.write.format('bigquery') \
        .option("table", "playground-s-11-318d2d4e.students.cities")\
        .option("temporaryGcsBucket",TEMPORARY_BUCKET_NAME )\
        .mode("append")\
        .save()
    
def read_table_as_df(dataset_name, table_name, schema_df):
    return spark.read.format("bigquery")\
        .option("table", f"{GCP_PROJECT_NAME}.{dataset_name}.{table_name}")\
        .schema(schema_df)\
        .load()


if __name__ == "__main__":
    sample_data = [ (12345, "CITY 1", "STATE 1", 704, "STANDARD"),
                (99999, "CITY 2", "STATE 2", 907, "STANDARD")
              ]

    schema = StructType(
        [
            StructField("RecordNumber", IntegerType(), True),
            StructField("City", StringType(), True),
            StructField("State", StringType(), True),
            StructField("ZipCodeType", IntegerType(), True),
            StructField("Zipcode", StringType(), True)
        ]
    )

    df = spark.createDataFrame(data=sample_data, schema=schema)
    store_df_as_table(df, BIGQUERY_DATESET_NAME, 'cities')
    print("Dataframe writing is completed")

    # reading data from bigquery
    df2 = read_table_as_df( BIGQUERY_DATESET_NAME, 'cities', schema)
    print("DataFrame read is completed")

    df2.printSchema()
    df2.show(5, truncate=False)