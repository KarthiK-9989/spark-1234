import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, max

# Get parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Initialize Spark and Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read the previously stored maximum id (or start with a default)
try:
    # Read last loaded ID from the correct path
    last_id_df = spark.read.text("s3://ouputbucket34/output/finaloutput29/last_loaded_id.txt")
    last_id = int(last_id_df.collect()[0][0])  # Get the ID as integer
except Exception as e:
    # If no file found, start with a default value (e.g., 0)
    last_id = 0  # Default value for first run

# Load only records where id > last_id
df = spark.read.format("csv") \
    .option("header", "true") \
    .load("s3://thirdbucket01/details.csv") \
    .filter(col("id") > last_id)  # Incremental load based on id

# Show loaded DataFrame for debugging
df.show()

# Perform transformation
df2 = df.select(
    col("id"),
    col("Salary"),
    when(col("Salary") > 500, "Rich").otherwise("Poor").alias("Income_Status")
)

# Write the transformed DataFrame to S3 in CSV format (incremental load)
df2.write.format("csv") \
    .option("header", "true") \
    .mode("append") \
    .save("s3://ouputbucket34/output/finaloutput29")

# Only update last_id if there are new records
if not df.isEmpty():
    new_last_id = df.agg(max(col("id"))).collect()[0][0]
    # Write the new maximum id back to the S3 file for the next run
    id_df = spark.createDataFrame([(new_last_id,)], ["id"])
    id_df.coalesce(1).write.mode("overwrite").text("s3://ouputbucket34/output/finaloutput29/last_loaded_id.txt")

# Commit the job
job.commit()
