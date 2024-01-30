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

# Script generated for node Curated customers
Curatedcustomers_node1706591451214 = glueContext.create_dynamic_frame.from_catalog(
    database="stedidb",
    table_name="customers_curated",
    transformation_ctx="Curatedcustomers_node1706591451214",
)

# Script generated for node Landing step trainer
Landingsteptrainer_node1706590833841 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://tam-step-trainer/landing/"], "recurse": True},
    transformation_ctx="Landingsteptrainer_node1706590833841",
)

# Script generated for node Join
Join_node1706591478841 = Join.apply(
    frame1=Landingsteptrainer_node1706590833841,
    frame2=Curatedcustomers_node1706591451214,
    keys1=["serialnumber"],
    keys2=["serialnumber"],
    transformation_ctx="Join_node1706591478841",
)

# Script generated for node Drop Fields
DropFields_node1706591594449 = DropFields.apply(
    frame=Join_node1706591478841,
    paths=[
        "email",
        "phone",
        "birthDay",
        "shareWithPublicAsOfDate",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "customerName",
        "shareWithFriendsAsOfDate",
        "`.serialNumber`",
        "lastUpdateDate",
    ],
    transformation_ctx="DropFields_node1706591594449",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1706594574246 = DynamicFrame.fromDF(
    DropFields_node1706591594449.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1706594574246",
)

# Script generated for node Amazon S3
AmazonS3_node1706591539392 = glueContext.getSink(
    path="s3://tam-step-trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1706591539392",
)
AmazonS3_node1706591539392.setCatalogInfo(
    catalogDatabase="stedidb", catalogTableName="step_trainer_trusted"
)
AmazonS3_node1706591539392.setFormat("json")
AmazonS3_node1706591539392.writeFrame(DropDuplicates_node1706594574246)
job.commit()
