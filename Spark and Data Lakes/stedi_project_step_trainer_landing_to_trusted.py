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

# Script generated for node customer_curated
customer_curated_node1673971736203 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://buckets-mingxingjin/stedi_project/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="customer_curated_node1673971736203",
)

# Script generated for node step_trainer_landing
step_trainer_landing_node1673971688997 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://buckets-mingxingjin/stedi_project/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_landing_node1673971688997",
)

# Script generated for node Change Schema (Apply Mapping)
ChangeSchemaApplyMapping_node1673971809250 = ApplyMapping.apply(
    frame=step_trainer_landing_node1673971688997,
    mappings=[
        ("sensorReadingTime", "bigint", "sensorReadingTime", "long"),
        ("serialNumber", "string", "st_serialNumber", "string"),
        ("distanceFromObject", "int", "distanceFromObject", "int"),
    ],
    transformation_ctx="ChangeSchemaApplyMapping_node1673971809250",
)

# Script generated for node Join
Join_node1673971872532 = Join.apply(
    frame1=ChangeSchemaApplyMapping_node1673971809250,
    frame2=customer_curated_node1673971736203,
    keys1=["st_serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="Join_node1673971872532",
)

# Script generated for node Drop Fields
DropFields_node1673971898422 = DropFields.apply(
    frame=Join_node1673971872532,
    paths=[
        "st_serialNumber",
        "birthDay",
        "shareWithPublicAsOfDate",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "customerName",
        "shareWithFriendsAsOfDate",
        "lastUpdateDate",
        "phone",
    ],
    transformation_ctx="DropFields_node1673971898422",
)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1673971929719 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1673971898422,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://buckets-mingxingjin/stedi_project/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="step_trainer_trusted_node1673971929719",
)

job.commit()
