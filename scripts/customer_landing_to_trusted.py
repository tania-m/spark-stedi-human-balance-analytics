# Expected data row totals: 

# Landing
# Customer: 956
# Accelerometer: 81273
# Step Trainer: 28680

# Trusted
# Customer: 482
# Accelerometer: 40981
# Step Trainer: 14460

# Curated
# Customer: 482
# Machine Learning: 43681

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
AmazonS3_node1706513818148 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://stedi-customers"], "recurse": True},
    transformation_ctx="AmazonS3_node1706513818148",
)

# Script generated for node Filter
Filter_node1706513898528 = Filter.apply(
    frame=AmazonS3_node1706513818148,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="Filter_node1706513898528",
)

# Script generated for node Amazon S3
AmazonS3_node1706513918114 = glueContext.getSink(
    path="s3://stedi-customers/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1706513918114",
)
AmazonS3_node1706513918114.setCatalogInfo(
    catalogDatabase="stedidb", catalogTableName="customer_trusted"
)
AmazonS3_node1706513918114.setFormat("json")
AmazonS3_node1706513918114.writeFrame(Filter_node1706513898528)
job.commit()