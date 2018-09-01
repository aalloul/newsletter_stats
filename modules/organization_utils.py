from boto3 import resource, client
import logging
from sys import stdout
from json import dumps

# Logging
logging.basicConfig(stream=stdout, format='%(asctime)s %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def _check_org_exists(s3object):
    from botocore.exceptions import ClientError
    try:
        s3object.load()
        return True
    except ClientError:
        return False


def create_organization(org):
    s3 = _get_s3_resource()
    obj = s3.Object("fstatsfiles", org+"/")

    if _check_org_exists(obj):
        logger.error("Organization already exists")
        raise Exception("Malformed request")

    res = obj.put(ACL='private', Body='')

    if 200 <= res["ResponseMetadata"]["HTTPStatusCode"] < 300:
        logger.info(f"Organization {org} was created at {res}")
        return {"result": 200}

    else:
        logger.error(f"Could not create organization {org}")
        logger.error(f"Response from S3 {dumps(res, indent=4)}")
        raise Exception(f"Could not create organization {org}")


def _get_objects(s3client, org, ct):
    if ct is None:
        res = s3client.list_objects_v2(Bucket="fstatsfiles", Prefix=org+"/")
    else:
        res = s3client.list_objects_v2(Bucket="fstatsfiles", Prefix=org+"/",
                                       ContinuationToken=ct)

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
        if 'ContinuationToken' in res:
            continuation_token = res['ContinuationToken']

    logger.debug("  Deleting files")
    obj = []
    while len(files_to_delete) > 0:
        obj.append({'Key': files_to_delete[0]})
        files_to_delete.pop(0)

        if len(obj) == 1000:
            s3client.delete_objects(Bucket='fstatsfiles',
                                    Delete={'Objects': obj})
            obj.clear()

    if len(obj) > 0:
        s3client.delete_objects(Bucket='fstatsfiles', Delete={'Objects': obj})


def delete_organization(org):
    s3 = _get_s3_client()
    obj = _get_s3_resource().Object("fstatsfiles", org+"/")

    if not _check_org_exists(obj):
        logger.error(f"Organization {org} does not exist")
        raise Exception("Organization does not exist")

    _empty_bucket(s3, org)

    logger.info(f"Bucket {org} was successfully deleted")
    return {"result": 200}


def _get_s3_resource():
    return resource('s3')


def _get_s3_client():
    return client('s3')


# if __name__ == "__main__":
#
# create_organization("org1")
# delete_organization("org")