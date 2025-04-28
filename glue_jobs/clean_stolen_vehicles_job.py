import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_date

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

input_path = "s3://nz-crime-data-pipeline/raw/stolen_vehicles.csv"
output_path = "s3://nz-crime-data-pipeline/clean/stolen_vehicles/"

df = spark.read.option("header", True).csv(input_path)

df_clean = df.withColumn("vehicle_id", col("vehicle_id").cast("int")) \
             .withColumn("model_year", col("model_year").cast("int")) \
             .withColumn("date_stolen", to_date(col("date_stolen"), "M/d/yy")) \
             .withColumn("make_id", col("make_id").cast("int")) \
             .withColumn("location_id", col("location_id").cast("int"))

df_clean.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)

job.commit()
