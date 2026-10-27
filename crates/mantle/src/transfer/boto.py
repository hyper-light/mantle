# The boto3 driver of `mantle bench transfer` (src/transfer/stock.rs writes it out and runs it): one
# process a case, which puts and gets the objects the harness asks for through a cell's S3 listener,
# their bytes fed to it and read from it over its standard input and output, so it holds no more
# than one object, of the small sets' at most 128 KiB, at a time.
#
#   python3 boto.py ENDPOINT REGION BUCKET
#
# On standard output first `ready`, once its client is made, so the harness times no start-up.
# Then on standard input, a line each: `put KEY LENGTH` followed by LENGTH bytes; `get KEY`; `quit`.
# On standard output: `ok` for a put taken; `len N` followed by N bytes for a get; `error CODE`
# for S3's refusal, its code. Credentials are the profile's the environment names.
import sys

import boto3
import botocore.exceptions
from botocore.config import Config

endpoint, region, bucket = sys.argv[1], sys.argv[2], sys.argv[3]
s3 = boto3.client(
    "s3",
    endpoint_url=endpoint,
    region_name=region,
    config=Config(s3={"addressing_style": "path"}),
)
stdin = sys.stdin.buffer
stdout = sys.stdout.buffer


def say(line):
    stdout.write(line.encode() + b"\n")
    stdout.flush()


def code(error):
    return error.response.get("Error", {}).get("Code", "Unknown")


say("ready")


while True:
    line = stdin.readline()
    if not line:
        break
    words = line.decode().split()
    if not words:
        continue
    if words[0] == "put":
        key, length = words[1], int(words[2])
        body = stdin.read(length)
        try:
            s3.put_object(Bucket=bucket, Key=key, Body=body)
            say("ok")
        except botocore.exceptions.ClientError as error:
            say("error " + code(error))
    elif words[0] == "get":
        try:
            body = s3.get_object(Bucket=bucket, Key=words[1])["Body"].read()
            say("len %d" % len(body))
            stdout.write(body)
            stdout.flush()
        except botocore.exceptions.ClientError as error:
            say("error " + code(error))
    elif words[0] == "quit":
        break
