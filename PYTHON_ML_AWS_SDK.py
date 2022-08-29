import boto3
import os
import shutil
from pyspark.sql import SparkSession

# Amazon Web Services' credentials with permissions only for S3 to download and upload Parquet files
AWS_ACCESS_KEY_ID = ""
AWS_SECRET_ACCESS_KEY = ""
AWS_BUCKET_NAME = ""
AWS_BUCKET_INITIALIZE_FOLDER = 'initialize'
AWS_BUCKET_RESULTS_FOLDER = 'results'
PARQUET_TEMPORARY_FOLDER = 'parquet'
CSV_FILE = 'report.csv'


def main():
    # We download a specific CSV file from an S3 Bucket in Amazon Web Services
    aws_download_files(AWS_BUCKET_INITIALIZE_FOLDER, AWS_BUCKET_NAME)

    convert_csv_to_parquet(AWS_BUCKET_INITIALIZE_FOLDER,
                           CSV_FILE, PARQUET_TEMPORARY_FOLDER)

    session = read_parquet_file(PARQUET_TEMPORARY_FOLDER)

    # Print a customized Query to analyze the Data Set.
    print_total_sales_per_product(session)

    # Print a customized Query to analyze the Data Set.
    print_sum_all_purchases(session)

    # The Parquet files are uploaded to an S3 Bucket in Amazon Web Services
    aws_upload_files(PARQUET_TEMPORARY_FOLDER, AWS_BUCKET_NAME)


def convert_csv_to_parquet(bucket_folder, csv_file, local_folder):
    print(
        f"Initializing process to convert CSV file to Parquet. CSV File name: {csv_file}")
    complete_source_path = bucket_folder + "/" + csv_file

    # Obtain the data set
    data_set = SparkSession.builder.getOrCreate().read.csv(
        complete_source_path, inferSchema=True, header=True)

    current_path = get_current_folder_path()
    complete_path = current_path + '/' + local_folder

    # If the folder already exists we delete it and later we create it again with the new Parquet file
    if os.path.exists(complete_path):
        shutil.rmtree(complete_path)

    # We create the new Parquet file from the CSV file
    data_set.write.parquet(local_folder)
    print(
        f"\nParquet file successfully created. File Path: {complete_path}\n")


def check_aws_connection():
    aws_s3 = boto3.client('s3',
                          aws_access_key_id=AWS_ACCESS_KEY_ID,
                          aws_secret_access_key=AWS_SECRET_ACCESS_KEY)


def print_total_sales_per_product(session):

    print("Initializing Query execution on Parquet file")
    print("Querying the total sales per product:\n")
    session.sql("SELECT PRODUCT, SUM(TOTAL_PURCHASED) FROM ParquetTable GROUP BY PRODUCT ORDER BY PRODUCT ASC").show(
        truncate=False)

    print("Query successfully performed\n")


def print_sum_all_purchases(session):

    print("Initializing Query execution on Parquet file")
    print("Querying the sum of all purchases:\n")
    session.sql("SELECT SUM(TOTAL_PURCHASED) FROM ParquetTable").show(
        truncate=False)

    print("Query successfully performed\n")


def read_parquet_file(file_name):
    session = SparkSession.builder.appName("myparquetnewfile").getOrCreate()
    parquetFile = session.read.parquet(file_name)
    parquetFile.createOrReplaceTempView("parquetTable")
    return session


def aws_upload_files(local_temporary_folder, bucket_name):
    print(
        f"Initializing process for uploading the newly created Parquet files to AWS S3. Bucket name: {bucket_name}")
    # We utilize our credentials to access the Amazon Web Services resources
    aws_sdk = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID,
                           aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    for root, dirs, files in os.walk(local_temporary_folder):
        for file in files:
            complete_aws_s3_file_path = AWS_BUCKET_RESULTS_FOLDER + '/' + file
            aws_sdk.upload_file(os.path.join(
                root,  file), bucket_name, complete_aws_s3_file_path)
            print(
                f"Uploading to AWS completed. File name: {complete_aws_s3_file_path}")

    print(
        f"Files successfully uploaded. AWS S3. Bucket Name: {bucket_name}\n")


def aws_download_files(bucket_folder, bucket_name):
    print(
        f"Initializing process for downloading files from AWS S3. Bucket name: {bucket_name}")
    # We utilize our credentials to access the Amazon Web Services resources
    aws_sdk = boto3.resource("s3",
                             aws_access_key_id=AWS_ACCESS_KEY_ID,
                             aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

    # We specify the Bucket from which we will download the Parquet files
    bucket = aws_sdk.Bucket(bucket_name)
    create_folder_for_bucket_contents(bucket_folder)

    for file in bucket.objects.filter(Prefix=bucket_folder):
        # We only download the contents but not the folder specified
        if file.key == (bucket_folder + '/'):
            continue
        bucket.download_file(file.key, file.key)
        print(f"Downliading from AWS completed. File name: {file.key}")
    print(
        f"Files successfully downloaded. AWS S3. Bucket Name: {bucket_name}\n")


def create_folder_for_bucket_contents(bucket_folder):
    current_path = get_current_folder_path()
    target_bucket_path = current_path + '/' + bucket_folder
    if not os.path.exists(target_bucket_path):
        os.makedirs(bucket_folder)
    return target_bucket_path


def get_current_folder_path():
    current_folder = os.getcwd()
    current_path = current_folder.replace('\\', '/')
    return current_path


def initialize():
    session = SparkSession.builder.getOrCreate()
    return session


if __name__ == "__main__":
    main()
