import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon S3
AmazonS3_node1706588367957 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://tam-accelerometer/landing/"], "recurse": True},
    transformation_ctx="AmazonS3_node1706588367957",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1706588441024 = glueContext.create_dynamic_frame.from_catalog(
    database="stedidb",
    table_name="customer_trusted",
    transformation_ctx="AWSGlueDataCatalog_node1706588441024",
)

# Script generated for node Join
Join_node1706588468901 = Join.apply(
    frame1=AWSGlueDataCatalog_node1706588441024,
    frame2=AmazonS3_node1706588367957,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1706588468901",
)

# Script generated for node Drop Fields
DropFields_node1706589397166 = DropFields.apply(
    frame=Join_node1706588468901,
    paths=[
        "phone",
        "email",
        "serialNumber",
        "birthDay",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "customerName",
        "lastUpdateDate",
        "shareWithPublicAsOfDate",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1706589397166",
)

# Script generated for node Amazon S3
AmazonS3_node1706588539880 = glueContext.getSink(
    path="s3://tam-accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1706588539880",
)
AmazonS3_node1706588539880.setCatalogInfo(
    catalogDatabase="stedidb", catalogTableName="accelerometer_trusted"
)
AmazonS3_node1706588539880.setFormat("json")
AmazonS3_node1706588539880.writeFrame(DropFields_node1706589397166)
job.commit()
