# gcloud dataproc jobs submit pyspark gs://playground-s-11-318d2d4e-dataproc/codes/read_cities.py\
#   --cluster=bluesea --region=us-central1 --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar\
#   --files="gs://playground-s-11-318d2d4e-dataproc/codes/configs/project_parameters.yaml,gs://playground-s-11-318d2d4e-dataproc/codes/bq_utils.py"

from bq_utils import *

spark = get_spark_session('read_cities')
cities_df = read_table_or_view(spark, "students.cities")

print(cities_df.printSchema())
print(cities_df.show(10, False))