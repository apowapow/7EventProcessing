import boto3
import argparse
import os.path
import json
import uuid

args = None

S3 = "s3"
SQS = "sqs"
SNS = "sns"

FILE_LOCATIONS = "locations.json"
REGION_NAME = "eu-west-1"
QUEUE_NAME = "apaltr-{0}".format(str(uuid.uuid4()))

KEY_X = "x"
KEY_Y = "y"
KEY_ID = "id"

KEY_MESSAGES = "Messages"
KEY_MD5 = "MD5OfBody"
KEY_BODY = "Body"
KEY_MESSAGE = "Message"


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
        # Part 1: task 1
        bucket = s3.Bucket(args.bucket_name)
        json_loc = get_json(bucket, FILE_LOCATIONS)

        # Part 1: task 2
        queue = sqs.create_queue(QueueName=QUEUE_NAME)
        queue_url = sqs.get_queue_url(QueueName=QUEUE_NAME)["QueueUrl"]
        queue_arn = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=['All'])['Attributes']['QueueArn']

        print("queue_url: {0}".format(queue_url))
        print("queue_arn: {0}".format(queue_arn))

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

        data = {}

        while True:
            response = sqs.receive_message(QueueUrl=queue_url)

            if KEY_MESSAGES in response:
                for message in response[KEY_MESSAGES]:
                    if message[KEY_MD5] not in data:
                        body = json.loads(message[KEY_BODY])
                        data[message[KEY_MD5]] = body[KEY_MESSAGE]
                        print("{0} : {1}".format(message[KEY_MD5], body[KEY_MESSAGE]))


    except Exception as e:
        print("Exception: {0}".format(e))

    finally:
        if queue and queue_url:
            sqs.delete_queue(QueueUrl=queue_url)


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
    parser.add_argument('-topic_arn', type=str)
    args = parser.parse_args()

    main()
