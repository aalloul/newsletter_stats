from boto3 import client
import logging
from sys import stdout

# Logging
logging.basicConfig(stream=stdout, format='%(asctime)s %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def _check_org_exists(s3client, name):
    from botocore.exceptions import ClientError
    try:
        s3client.head_bucket(Bucket=name)
        return True
    except ClientError:
        return False


def create_organization(org):
    s3 = _get_s3_client()

    if _check_org_exists(s3, org):
        logger.error("Organization already exists")
        raise Exception("Malformed request")

    res = s3.create_bucket(ACL='private', Bucket=org)

    if 'Location' not in res or res['Location'] == '':
        logger.error("Unexpected response from s3, Location not found in "
                     "result")
        logger.error(f"Answer was {res}")
        raise Exception("Unexpected answer from S3")

    logger.info(f"Organization {org} was created at {res['Location']}")

    return {"response": 200}


def _get_objects(s3client, org, ct):
    if ct is None:
        res = s3client.list_objects_v2(Bucket=org)
    else:
        res = s3client.list_objects_v2(Bucket=org, ContinuationToken=ct)

    if 'KeyCount' not in res:
        logger.error(f"KeyCount not found in response to "
                     f"list_objects_v2 for organization {org}")
        raise Exception("Internal Error - KeyCount missing in "
                        "response from list_objects_v2")

    if res['KeyCount'] == 0:
        return None

    return res


def _empty_bucket(s3client, org):
    is_trunc = True
    files_to_delete = []
    continuation_token = None

    logger.debug("  Listing bucket content")
    while is_trunc:
        res = _get_objects(s3client, org, continuation_token)
        if res is None:
            break
        files_to_delete += [r['Key'] for r in res['Contents']]
        is_trunc = res['IsTruncated']
        continuation_token = res['ContinuationToken']

    logger.debug("  Deleting files")
    obj = []
    while len(files_to_delete) > 0:
        obj.append({'Key': files_to_delete[0]})
        files_to_delete.pop(0)

        if len(obj) == 1000:
            s3client.delete_objects(Bucket='string', Delete={'Objects': obj})
            obj.clear()


def delete_organization(org):
    s3 = _get_s3_client()

    if not _check_org_exists(s3, org):
        logger.error(f"Organization {org} does not exist")
        raise Exception("Organization does not exist")

    _empty_bucket(s3, org)

    s3.delete_bucket(Bucket=org)
    logger.info(f"Bucket {org} was successfully deleted")
    return True


def _get_s3_client():
    global DEBUG
    if DEBUG:
        return client('s3', endpoint_url='http://localhost:4572/')
    else:
        return client('s3')


# if __name__ == "__main__":
DEBUG = True
create_organization("org1")