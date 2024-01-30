import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


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

# Script generated for node Customers curated
Customerscurated_node1706597148395 = glueContext.create_dynamic_frame.from_catalog(
    database="stedidb",
    table_name="customers_curated",
    transformation_ctx="Customerscurated_node1706597148395",
)

# Script generated for node Step trainer trusted
Steptrainertrusted_node1706597168416 = glueContext.create_dynamic_frame.from_catalog(
    database="stedidb",
    table_name="step_trainer_trusted",
    transformation_ctx="Steptrainertrusted_node1706597168416",
)

# Script generated for node Accelerometer trusted
Accelerometertrusted_node1706597186122 = glueContext.create_dynamic_frame.from_catalog(
    database="stedidb",
    table_name="accelerometer_trusted",
    transformation_ctx="Accelerometertrusted_node1706597186122",
)

# Script generated for node SQL Query
SqlQuery0 = """
SELECT step_trainer_trusted.serialnumber, step_trainer_trusted.sensorreadingtime, step_trainer_trusted.distancefromobject, accelerometer_trusted.x, accelerometer_trusted.y, accelerometer_trusted.z
FROM customers_curated, step_trainer_trusted, accelerometer_trusted 
WHERE customers_curated.serialnumber = step_trainer_trusted.serialnumber
AND customers_curated.email = accelerometer_trusted.user
AND accelerometer_trusted.timestamp = step_trainer_trusted.sensorreadingtime
"""
SQLQuery_node1706597231035 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "accelerometer_trusted": Accelerometertrusted_node1706597186122,
        "customers_curated": Customerscurated_node1706597148395,
        "step_trainer_trusted": Steptrainertrusted_node1706597168416,
    },
    transformation_ctx="SQLQuery_node1706597231035",
)

# Script generated for node Amazon S3
AmazonS3_node1706598126376 = glueContext.getSink(
    path="s3://tam-step-trainer/machine-learning-curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1706598126376",
)
AmazonS3_node1706598126376.setCatalogInfo(
    catalogDatabase="stedidb", catalogTableName="machine_learning_curated"
)
AmazonS3_node1706598126376.setFormat("json")
AmazonS3_node1706598126376.writeFrame(SQLQuery_node1706597231035)
job.commit()
