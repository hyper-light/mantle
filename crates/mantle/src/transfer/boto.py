# The boto3 driver of `mantle bench transfer` (src/transfer/stock.rs writes it out and runs it): one
# process a case, which puts and gets the objects the harness asks for through a cell's S3 listener,
# their bytes fed to it and read from it over its standard input and output, so it holds no more
# than one small object, of the small sets' at most 128 KiB, or one part of a large one at a time.
#
#   python3 boto.py ENDPOINT REGION BUCKET
#
# On standard output first `ready`, once its client is made, so the harness times no start-up.
# Then on standard input, a line each: `put KEY LENGTH` followed by LENGTH bytes; `get KEY`;
# `mput KEY LENGTH PART`, a multipart upload of LENGTH bytes in parts of PART, answered a line for
# its creation, then a line for each part once its bytes came and it was put, then one for its
# completion, the harness sending a part's bytes only once the line before said `ok`; `quit`.
# On standard output: `ok` for a put, an upload made or a part taken; `len N` followed by N bytes
# for a get; `error CODE` for S3's refusal, its code, after which a multipart upload is aborted and
# takes nothing more. Credentials are the profile's the environment names.
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


# The checksums a part's answer may name, which CompleteMultipartUpload's Part takes.
PART_CHECKSUMS = (
    "ChecksumCRC32",
    "ChecksumCRC32C",
    "ChecksumCRC64NVME",
    "ChecksumSHA1",
    "ChecksumSHA256",
)


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
    elif words[0] == "mput":
        key, length, part = words[1], int(words[2]), int(words[3])
        try:
            upload = s3.create_multipart_upload(Bucket=bucket, Key=key)["UploadId"]
        except botocore.exceptions.ClientError as error:
            say("error " + code(error))
            continue
        say("ok")
        parts, at, number, failed = [], 0, 0, False
        while at < length:
            number += 1
            body = stdin.read(min(part, length - at))
            at += len(body)
            try:
                taken = s3.upload_part(
                    Bucket=bucket, Key=key, UploadId=upload, PartNumber=number, Body=body
                )
            except botocore.exceptions.ClientError as error:
                s3.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload)
                say("error " + code(error))
                failed = True
                break
            # An upload made with a checksum algorithm, boto3's default since 1.36, is completed
            # with each part's checksum, as s3transfer completes one.
            entry = {"ETag": taken["ETag"], "PartNumber": number}
            entry.update({k: taken[k] for k in PART_CHECKSUMS if k in taken})
            parts.append(entry)
            say("ok")
        if failed:
            continue
        try:
            s3.complete_multipart_upload(
                Bucket=bucket, Key=key, UploadId=upload, MultipartUpload={"Parts": parts}
            )
            say("ok")
        except botocore.exceptions.ClientError as error:
            s3.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload)
            say("error " + code(error))
    elif words[0] == "quit":
        break
