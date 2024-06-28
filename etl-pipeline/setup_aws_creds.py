from prefect_aws import AwsCredentials
import os
from dotenv import load_dotenv
import boto3
from prefect.infrastructure import DockerContainer

load_dotenv()

def create_docker_block():
    # Create DockerContainer block
    docker_block = DockerContainer(
        image="crypto-etl:latest",
        image_pull_policy="ALWAYS",
        auto_remove=True,
    )

    # Save the block
    docker_block.save("crypto-etl", overwrite=True)
    print("Docker Container block 'crypto-etl' has been created.")

def create_aws_credentials_block():
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")

    aws_creds = AwsCredentials(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )
    aws_creds.save("my-aws-creds", overwrite=True)
    print(f"AWS Credentials block 'my-aws-creds' has been created.")
    print(f"Access Key ID: {aws_access_key_id}")
    print(f"Secret Access Key length: {len(aws_secret_access_key)}")

    # Test the credentials
    try:
        sts = boto3.client('sts',
                           aws_access_key_id=aws_access_key_id,
                           aws_secret_access_key=aws_secret_access_key)
        response = sts.get_caller_identity()
        print(f"AWS credentials are valid. Account ID: {response['Account']}")
    except Exception as e:
        print(f"Error testing AWS credentials: {str(e)}")

if __name__ == "__main__":
    create_docker_block()
    create_aws_credentials_block()