# libraries to load the environment variables
import os
from dotenv import load_dotenv
load_dotenv()

#import the different steps to execute
from src.extract import extract_transactional_data
from src.transform import identify_and_remove_duplicates
from src.load_to_s3 import load_df_to_s3

# fetch the environment variables from the .env file using the getenv function
aws_access_key = os.getenv("aws_access_key")
aws_secret_access_key = os.getenv("aws_secret_access_key")
# define the key and s3 bucket
key = "etl/cw_online_trans_final.csv"
aws_s3_bucket = "waia-march-bootcamp"

# extracting the online transaction data from the tables in the database and carrying out some transformation tasks
online_trans = extract_transactional_data()
print("Extracting the data")

# remove duplicates
ot_wout_duplicates = identify_and_remove_duplicates(online_trans)
print("Removing any duplicates")

# load the data to s3
print("Loading to s3 bucket")
load_df_to_s3(ot_wout_duplicates, key, aws_s3_bucket, aws_access_key, aws_secret_access_key)
print("Loaded to s3 bucket is finished")
