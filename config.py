import os 
from dotenv import load_dotenv


load_dotenv() #reads the .env file and loads the variables

#AWS credentails
aws_access_key = os.getenv('ACCESS_KEY')
aws_secret_key = os.getenv('SECRET_KEY')
region = os.getenv('REGION')

bucket='retailio-s-data-lake' #bucket name created in s3

#automatically reads all csv file in the data folder
dataset = {}
for file in os.listdir('data'):
    if file.endswith('.csv'):
        name = os.path.splitext(file)[0]   
        path = os.path.join('data', file)     
        dataset[name] = path


