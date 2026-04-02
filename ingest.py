#import all libraries required 
import os
import boto3
import pandas as pd
import awswrangler as wr
from config import aws_access_key, aws_secret_key, region, bucket, dataset

#function to upload csv file as parquet to s3
def upload_to_s3():
    # Create an AWS session using credentials from .env
    session = boto3.Session(
        aws_access_key_id = aws_access_key,
        aws_secret_access_key = aws_secret_key,
        region_name = region
    )
    #looping thru each dataset and upload to s3
    for name, path in dataset.items():
        if os.path.exists(path):
            df = pd.read_csv(path, encoding='latin1') #reading the csv files into dataframe
            #Upload the DataFrame to S3 as parquet
            wr.s3.to_parquet(
                df = df,
                path = f's3://{bucket}/raw/{path}',
                index = False,
                mode = 'overwrite',
                dataset = True,
                boto3_session = session

            )
            print(f'{name} successfully uploaded')
        else:
            print(f'{path} does not exist')