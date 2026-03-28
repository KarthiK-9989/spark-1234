import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


dynamic_frame=glueContext.create_dynamic_frame.from_catalog(
    
    database="march26db",
    table_name="informationmarch26_us_east_1_001"
    )

df=dynamic_frame.toDF()

df.write.format("csv").option("header",True).option("path","s3://march26-us-east-1-001/march29op").save()



job.commit()