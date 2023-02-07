import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
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

# Script generated for node accelerometer_landing
accelerometer_landing_node1673883461420 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://buckets-mingxingjin/stedi_project/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_landing_node1673883461420",
)

# Script generated for node customer_trusted
customer_trusted_node1673998431529 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://buckets-mingxingjin/stedi_project/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="customer_trusted_node1673998431529",
)

# Script generated for node Join
accelerometer_landing_node1673883461420DF = (
    accelerometer_landing_node1673883461420.toDF()
)
customer_trusted_node1673998431529DF = customer_trusted_node1673998431529.toDF()
Join_node1673883558726 = DynamicFrame.fromDF(
    accelerometer_landing_node1673883461420DF.join(
        customer_trusted_node1673998431529DF,
        (
            accelerometer_landing_node1673883461420DF["user"]
            == customer_trusted_node1673998431529DF["email"]
        ),
        "left",
    ),
    glueContext,
    "Join_node1673883558726",
)

# Script generated for node SQL Query
SqlQuery1510 = """
select * from myDataSource
where timestamp >= shareWithResearchAsOfDate
"""
SQLQuery_node1673998741502 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1510,
    mapping={"myDataSource": Join_node1673883558726},
    transformation_ctx="SQLQuery_node1673998741502",
)

# Script generated for node Drop Fields
DropFields_node1673883578516 = DropFields.apply(
    frame=SQLQuery_node1673998741502,
    paths=[
        "shareWithFriendsAsOfDate",
        "phone",
        "lastUpdateDate",
        "email",
        "customerName",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "birthDay",
        "shareWithPublicAsOfDate",
        "serialNumber",
    ],
    transformation_ctx="DropFields_node1673883578516",
)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1673883592559 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1673883578516,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://buckets-mingxingjin/stedi_project/accelerometer/trusted_v2/",
        "partitionKeys": [],
    },
    transformation_ctx="accelerometer_trusted_node1673883592559",
)

job.commit()
