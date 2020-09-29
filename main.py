import boto3
import argparse
import os.path
import json


args = None

FILE_LOCATIONS = "locations.json"


def main():
    session = boto3.Session(
        aws_access_key_id=args.aws_public,
        aws_secret_access_key=args.aws_secret,
    )

    s3 = session.resource('s3')

    bucket = s3.Bucket(args.bucket_name)
    json_loc = get_json(bucket, FILE_LOCATIONS)

    print(json_loc)


def get_json(bucket, file_name):
    maybe_download_file(bucket, file_name)

    with open(file_name, 'r') as f:
        return json.loads(f.read())


def maybe_download_file(bucket, file_name):
    if not os.path.isfile(file_name):
        print("Downloading '{0}' ... ".format(file_name), end="")
        bucket.download_file(file_name, file_name)
        print("DONE.")
    else:
        print("File '{0}' already downloaded.".format(file_name))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-aws_public', type=str)
    parser.add_argument('-aws_secret', type=str)
    parser.add_argument('-bucket_name', type=str)
    args = parser.parse_args()

    main()
