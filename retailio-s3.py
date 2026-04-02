import os 
import boto3
import pandas as pd
import awswrangler as wr
from dotenv import load_dotenv


load_dotenv() #reads the .env file and loads the variables

#AWS credentails
aws_access_key = os.getenv('ACCESS_KEY')
aws_secret_key = os.getenv('SECRET_KEY')
aws_region = os.getenv('REGION')

bucket='retailio-s-data-lake' #S3 bucket name 


session = boto3.Session(
    aws_access_key_id = aws_access_key,
    aws_secret_access_key = aws_secret_key,
    region_name = aws_region
    
)


dataset = {
    "customers": 'data/customer_V2.csv',
    "product": 'data/product_V2.csv',
    "sales": 'data/sales_V2.csv'
}


for name, path in dataset.items():
    if os.path.exists(path):
        df = pd.read_csv(path, encoding='latin1')
        wr.s3.to_parquet(
            df = df,
            path = f"s3://{bucket}/raw/{name}",
            index = False,
            mode = 'overwrite',
            dataset = True,
            boto3_session = session 
        )
        print(f"{name} uploaded successfully")
    else:
        print(f'{path} does not exist')
