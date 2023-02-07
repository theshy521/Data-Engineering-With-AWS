# Summary:
As a data engineer on the STEDI Step Trainer team, you'll need to extract the data produced by the STEDI Step Trainer sensors and the mobile app, and curate them into a data lakehouse solution on AWS so that Data Scientists can train the learning model.

# Step:
1. Load data from public s3 buckets or upload data directly.

2. Run the proper python scripts step by step:
  * customer_landing_to_trusted.py
  * accelerometer_landing_to_trusted.py
  * customer_trusted_to_curated.py
  * step_trainer_landing_to_trusted.py
  * machine_learning_curated.py
