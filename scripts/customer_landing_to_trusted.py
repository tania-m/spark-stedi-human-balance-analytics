# Sanitize the Customer data from the Website (Landing Zone) 
# and only store the Customer Records who agreed to share 
# their data for research purposes (Trusted Zone) - creating 
# a Glue Table called customer_trusted.

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon S3
AmazonS3_node1706583571219 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://tam-customers/landing/"], "recurse": True},
    transformation_ctx="AmazonS3_node1706583571219",
)

# Script generated for node Filter
Filter_node1706583589676 = Filter.apply(
    frame=AmazonS3_node1706583571219,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="Filter_node1706583589676",
)

# Script generated for node Amazon S3
AmazonS3_node1706583597517 = glueContext.getSink(
    path="s3://tam-customers/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1706583597517",
)
AmazonS3_node1706583597517.setCatalogInfo(
    catalogDatabase="stedidb", catalogTableName="customer_trusted"
)
AmazonS3_node1706583597517.setFormat("json")
AmazonS3_node1706583597517.writeFrame(Filter_node1706583589676)
job.commit()
