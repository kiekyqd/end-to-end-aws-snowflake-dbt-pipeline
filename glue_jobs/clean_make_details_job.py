import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

input_path = "s3://nz-crime-data-pipeline/raw/make_details.csv"
output_path = "s3://nz-crime-data-pipeline/clean/make_details/"

df = spark.read.option("header", True).csv(input_path)

df_clean = df.withColumn("make_id", col("make_id").cast("int"))

df_clean.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)

job.commit()
