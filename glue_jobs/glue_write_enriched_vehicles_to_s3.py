import sys 
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import to_date, col


# Step 1: Glue job setup (context, args, session)
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import to_date, col

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Step 2: Read cleaned CSVs from S3 (previously processed)
locations_df = spark.read.option("header", True).csv("s3://nz-crime-data-pipeline/clean/locations/")
makes_df = spark.read.option("header", True).csv("s3://nz-crime-data-pipeline/clean/make_details/")
vehicles_df = spark.read.option("header", True).csv("s3://nz-crime-data-pipeline/clean/stolen_vehicles/")

# Step 3: Cast data types for reliable joins and downstream use
vehicles_df = vehicles_df.withColumn("make_id", col("make_id").cast("int")) \
                         .withColumn("location_id", col("location_id").cast("int")) \
                         .withColumn("model_year", col("model_year").cast("int")) \
                         .withColumn("date_stolen", to_date("date_stolen", "yyyy-MM-dd"))

locations_df = locations_df.withColumn("location_id", col("location_id").cast("int")) \
                           .withColumn("population", col("population").cast("int")) \
                           .withColumn("density", col("density").cast("double"))

makes_df = makes_df.withColumn("make_id", col("make_id").cast("int"))

# Step 4: Join vehicles with make and location metadata
enriched_df = vehicles_df \
    .join(makes_df, on="make_id", how="left") \
    .join(locations_df, on="location_id", how="left")

# Step 5: Write enriched data to S3 (for Snowflake COPY INTO)
# This creates partitioned CSV files that will be loaded into Snowflake later
(
    enriched_df.write
    .option("header", True)
    .mode("overwrite")
    .csv("s3://nz-crime-data-pipeline/final/enriched_vehicles/")
)

# Finalize the Glue job
job.commit()
