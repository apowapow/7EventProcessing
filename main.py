import boto3
import argparse
import os.path
import json
import csv
import uuid
from datetime import datetime, timedelta
from collections import deque
from pprint import pprint

args = None
data_loc = {}

S3 = "s3"
SQS = "sqs"
SNS = "sns"

FILE_LOCATIONS = "locations.json"
FILE_OUTPUT_CSV = "output.csv"
REGION_NAME = "eu-west-1"
QUEUE_NAME = "apaltr-{0}".format(str(uuid.uuid4()))

KEY_X = "x"
KEY_Y = "y"
KEY_ID = "id"

KEY_MESSAGES = "Messages"
KEY_MD5 = "MD5OfBody"
KEY_BODY = "Body"
KEY_MESSAGE = "Message"

KEY_LOCATION_ID = "locationId"
KEY_EVENT_ID = "eventId"
KEY_VALUE = "value"
KEY_AVERAGE = "average"
KEY_TIMESTAMP = "timestamp"

CACHE_MAX_LEN = 1000
COUNT_PROGRESS_INTERVAL = 25


def main():
    session = boto3.Session(
        aws_access_key_id=args.aws_public,
        aws_secret_access_key=args.aws_secret,
        region_name=REGION_NAME
    )

    s3 = session.resource(S3)
    sqs = session.client(SQS)
    sns = session.client(SNS)

    queue = None
    queue_url = None
    queue_arn = None

    try:
        bucket = s3.Bucket(args.bucket_name)

        loc = get_json(bucket, FILE_LOCATIONS)
        loc_monitor = [d[KEY_ID] for d in loc]

        # Part 1: task 2
        queue = sqs.create_queue(QueueName=QUEUE_NAME)
        queue_url = sqs.get_queue_url(QueueName=QUEUE_NAME)["QueueUrl"]
        queue_arn = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=['All'])['Attributes']['QueueArn']

        policy_document = {
            'Version': '2012-10-17',
            'Statement': [{
                'Sid': f'allow-subscription-{args.topic_arn}',
                'Effect': 'Allow',
                'Principal': {'AWS': '*'},
                'Action': 'SQS:SendMessage',
                'Resource': f'{queue_arn}',
                'Condition': {
                    'ArnEquals': {'aws:SourceArn': f'{args.topic_arn}'}
                }
            }]
        }
        policy_json = json.dumps(policy_document)
        sqs.set_queue_attributes(QueueUrl=queue_url, Attributes={"Policy": policy_json})

        subscription = sns.subscribe(
            TopicArn=args.topic_arn,
            Protocol=SQS,
            Endpoint=queue_arn
        )
        subscription_arn = subscription['SubscriptionArn']

        count = 0
        time_stop = datetime.now() + timedelta(hours=1)
        print("Collecting data until {0} ...".format(time_stop))

        while datetime.now() < time_stop:
            response = sqs.receive_message(QueueUrl=queue_url)

            if KEY_MESSAGES in response:
                for message in response[KEY_MESSAGES]:
                    body = json.loads(message[KEY_BODY])
                    body_message = json.loads(body[KEY_MESSAGE])

                    msg_loc = body_message[KEY_LOCATION_ID]
                    msg_eve = body_message[KEY_EVENT_ID]
                    msg_val = body_message[KEY_VALUE]
                    msg_tim = body_message[KEY_TIMESTAMP]

                    msg_time_datetime = datetime.fromtimestamp(msg_tim / 1000)
                    msg_time_format = msg_time_datetime.strftime("%Y-%m-%d-%H-%M")

                    if msg_loc in loc_monitor:
                        # key by location id
                        if msg_loc not in data_loc:
                            data_loc[msg_loc] = {}

                        # key by minute of day within location id
                        if msg_time_format not in data_loc[msg_loc]:
                            data_loc[msg_loc][msg_time_format] = []

                        # ignore duplicates
                        for elem in data_loc[msg_loc][msg_time_format]:
                            if elem[KEY_EVENT_ID] == msg_eve:
                                continue

                        data_loc[msg_loc][msg_time_format].append(
                            {KEY_EVENT_ID: msg_eve, KEY_VALUE: msg_val, KEY_TIMESTAMP: msg_tim})
                        count += 1

                        if count % COUNT_PROGRESS_INTERVAL == 0:
                            print("  {0}".format(count))

        print("DONE")
        write_averages(data_loc, FILE_OUTPUT_CSV)

    except Exception as e:
        print("Exception: {0}".format(e))

    finally:
        if queue and queue_url:
            sqs.delete_queue(QueueUrl=queue_url)


def write_averages(data, file_name):
    delete_file_if_exists(file_name)
    data_csv = [[KEY_LOCATION_ID, KEY_TIMESTAMP, KEY_AVERAGE]]

    for key_location, location_data in data.items():
        for key_time, time_data in location_data.items():
            avg = 0
            count = 0
            for elem in time_data:
                avg += elem[KEY_VALUE]
                count += 1
            avg = avg / count

            timestamp = time_data[0][KEY_TIMESTAMP]
            timestamp_datetime = datetime.fromtimestamp(timestamp / 1000)
            timestamp_format = timestamp_datetime.strftime("%Y-%m-%d %H:%M")

            data_csv.append([key_location, timestamp_format, avg])

    print("Writing average to '{0}' ... ".format(file_name), end="")
    with open(file_name, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(data_csv)
    print("DONE")


def get_json(bucket, file_name):
    delete_file_if_exists(file_name)
    download_file(bucket, file_name)

    with open(file_name, 'r') as f:
        return json.loads(f.read())


def download_file(bucket, file_name):
    print("Downloading '{0}' ... ".format(file_name), end="")
    bucket.download_file(file_name, file_name)
    print("DONE")


def delete_file_if_exists(file_name):
    if os.path.isfile(file_name):
        print("Deleting existing copy of '{0}' ... ".format(file_name), end="")
        os.remove(file_name)
        print("DONE")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-aws_public', type=str)
    parser.add_argument('-aws_secret', type=str)
    parser.add_argument('-bucket_name', type=str)
    parser.add_argument('-topic_arn', type=str)
    args = parser.parse_args()

    main()
