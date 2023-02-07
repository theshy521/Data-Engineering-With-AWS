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

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1674005379468 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://buckets-mingxingjin/stedi_project/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_trusted_node1674005379468",
)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1674005555708 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://buckets-mingxingjin/stedi_project/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_trusted_node1674005555708",
)

# Script generated for node Join
Join_node1674005636821 = Join.apply(
    frame1=step_trainer_trusted_node1674005555708,
    frame2=accelerometer_trusted_node1674005379468,
    keys1=["email", "sensorReadingTime"],
    keys2=["user", "timeStamp"],
    transformation_ctx="Join_node1674005636821",
)

# Script generated for node step_trainer_curated
step_trainer_curated_node1674005721900 = glueContext.write_dynamic_frame.from_options(
    frame=Join_node1674005636821,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://buckets-mingxingjin/stedi_project/machine_learning/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="step_trainer_curated_node1674005721900",
)

job.commit()
