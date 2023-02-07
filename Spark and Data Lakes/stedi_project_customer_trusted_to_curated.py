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

# Script generated for node accelerometer_landing
accelerometer_landing_node1673966552488 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://buckets-mingxingjin/stedi_project/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_landing_node1673966552488",
)

# Script generated for node customer_trusted
customer_trusted_node1673966450967 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://buckets-mingxingjin/stedi_project/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="customer_trusted_node1673966450967",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1673970997777 = DynamicFrame.fromDF(
    customer_trusted_node1673966450967.toDF().dropDuplicates(["email"]),
    glueContext,
    "DropDuplicates_node1673970997777",
)

# Script generated for node Join
Join_node1673966673462 = Join.apply(
    frame1=accelerometer_landing_node1673966552488,
    frame2=DropDuplicates_node1673970997777,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1673966673462",
)

# Script generated for node SQL Query
SqlQuery1672 = """
select * from myDataSource
where timeStamp >= shareWithResearchAsOfDate
"""
SQLQuery_node1673971201657 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1672,
    mapping={"myDataSource": Join_node1673966673462},
    transformation_ctx="SQLQuery_node1673971201657",
)

# Script generated for node Drop Fields
DropFields_node1673966753582 = DropFields.apply(
    frame=SQLQuery_node1673971201657,
    paths=["z", "timeStamp", "user", "y", "x"],
    transformation_ctx="DropFields_node1673966753582",
)

# Script generated for node customer_curated
customer_curated_node1673966776696 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1673966753582,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://buckets-mingxingjin/stedi_project/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="customer_curated_node1673966776696",
)

job.commit()
