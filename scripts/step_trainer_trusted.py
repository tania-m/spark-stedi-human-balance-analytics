"""
    Create a table `customers_curated` containing
    accelerometer data for customers who hae agreed
    to have their data used for research
"""

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
AmazonS3_node1706590833841 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://tam-step-trainer/landing/"], "recurse": True},
    transformation_ctx="AmazonS3_node1706590833841",
)

# Script generated for node Curated customers
Curatedcustomers_node1706591451214 = glueContext.create_dynamic_frame.from_catalog(
    database="stedidb",
    table_name="customers_curated",
    transformation_ctx="Curatedcustomers_node1706591451214",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1706591654650 = ApplyMapping.apply(
    frame=Curatedcustomers_node1706591451214,
    mappings=[
        ("serialnumber", "string", "right_serialnumber", "string"),
        ("birthday", "string", "right_birthday", "string"),
        ("sharewithpublicasofdate", "long", "right_sharewithpublicasofdate", "long"),
        (
            "sharewithresearchasofdate",
            "long",
            "right_sharewithresearchasofdate",
            "long",
        ),
        ("registrationdate", "long", "right_registrationdate", "long"),
        ("customername", "string", "right_customername", "string"),
        ("sharewithfriendsasofdate", "long", "right_sharewithfriendsasofdate", "long"),
        ("email", "string", "right_email", "string"),
        ("lastupdatedate", "long", "right_lastupdatedate", "long"),
        ("phone", "string", "right_phone", "string"),
    ],
    transformation_ctx="RenamedkeysforJoin_node1706591654650",
)

# Script generated for node Join
Join_node1706591478841 = Join.apply(
    frame1=AmazonS3_node1706590833841,
    frame2=RenamedkeysforJoin_node1706591654650,
    keys1=["serialnumber"],
    keys2=["right_serialnumber"],
    transformation_ctx="Join_node1706591478841",
)

# Script generated for node Drop Fields
DropFields_node1706591594449 = DropFields.apply(
    frame=Join_node1706591478841,
    paths=[
        "right_serialnumber",
        "right_birthday",
        "right_sharewithpublicasofdate",
        "right_sharewithresearchasofdate",
        "right_registrationdate",
        "right_customername",
        "right_sharewithfriendsasofdate",
        "right_email",
        "right_lastupdatedate",
        "right_phone",
    ],
    transformation_ctx="DropFields_node1706591594449",
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
AmazonS3_node1706591539392.writeFrame(DropFields_node1706591594449)
job.commit()
