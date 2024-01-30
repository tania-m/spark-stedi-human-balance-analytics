import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customers trusted
Customerstrusted_node1706589691412 = glueContext.create_dynamic_frame.from_catalog(
    database="stedidb",
    table_name="customer_trusted",
    transformation_ctx="Customerstrusted_node1706589691412",
)

# Script generated for node Accelerometer trusted
Accelerometertrusted_node1706589714181 = glueContext.create_dynamic_frame.from_catalog(
    database="stedidb",
    table_name="accelerometer_trusted",
    transformation_ctx="Accelerometertrusted_node1706589714181",
)

# Script generated for node Join
Join_node1706589815128 = Join.apply(
    frame1=Accelerometertrusted_node1706589714181,
    frame2=Customerstrusted_node1706589691412,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1706589815128",
)

# Script generated for node Drop Fields
DropFields_node1706590245077 = DropFields.apply(
    frame=Join_node1706589815128,
    paths=["z", "y", "x", "user", "timestamp"],
    transformation_ctx="DropFields_node1706590245077",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1706593384900 = DynamicFrame.fromDF(
    DropFields_node1706590245077.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1706593384900",
)

# Script generated for node Amazon S3
AmazonS3_node1706589928409 = glueContext.getSink(
    path="s3://tam-customers/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1706589928409",
)
AmazonS3_node1706589928409.setCatalogInfo(
    catalogDatabase="stedidb", catalogTableName="customers_curated"
)
AmazonS3_node1706589928409.setFormat("json")
AmazonS3_node1706589928409.writeFrame(DropDuplicates_node1706593384900)
job.commit()
