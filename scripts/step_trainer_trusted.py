import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


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

# Script generated for node SQL Query
SqlQuery0 = """
SELECT step_trainer_landing.sensorReadingTime, step_trainer_landing.serialNumber, step_trainer_landing.distanceFromObject
FROM customers_curated
INNER JOIN step_trainer_landing ON customers_curated.serialnumber = step_trainer_landing.serialnumber;
"""
SQLQuery_node1706595778199 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "customers_curated": Curatedcustomers_node1706591451214,
        "step_trainer_landing": Landingsteptrainer_node1706590833841,
    },
    transformation_ctx="SQLQuery_node1706595778199",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1706594574246 = DynamicFrame.fromDF(
    SQLQuery_node1706595778199.toDF().dropDuplicates(),
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
