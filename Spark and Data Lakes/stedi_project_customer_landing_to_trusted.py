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

# Script generated for node customer_landing
customer_landing_node1673882375662 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": True},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://buckets-mingxingjin/stedi_project/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="customer_landing_node1673882375662",
)

# Script generated for node Filter
Filter_node1673882658122 = Filter.apply(
    frame=customer_landing_node1673882375662,
    f=lambda row: (row["shareWithResearchAsOfDate"] > 0),
    transformation_ctx="Filter_node1673882658122",
)

# Script generated for node customer_trusted
customer_trusted_node1673882693835 = glueContext.write_dynamic_frame.from_options(
    frame=Filter_node1673882658122,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://buckets-mingxingjin/stedi_project/customer/trusted_v2/",
        "partitionKeys": [],
    },
    transformation_ctx="customer_trusted_node1673882693835",
)

job.commit()
