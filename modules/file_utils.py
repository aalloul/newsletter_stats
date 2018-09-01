from base64 import b64decode
from boto3 import resource
import logging
from sys import stdout
from json import dumps

# Logging
logging.basicConfig(stream=stdout, format='%(asctime)s %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

URL = "www.blabla.com"


def _check_file_exists(s3obj):
    try:
        s3obj.load()
    except Exception:
        return False

    return True


def upload_file(filename, fcontent, org):
    global URL

    s3 = _get_s3_resource()
    obj = s3.Object("fstatsfiles", f"{org}/{filename}")
    if _check_file_exists(obj):
        logger.error(f"File {filename} in organization {org} already exists")
        raise Exception(f"File {filename} in organization {org} already exists")

    res = obj.put(ACL='private', Body=b64decode(fcontent))

    if 200 <= res["ResponseMetadata"]["HTTPStatusCode"] < 300:
        logger.info(f"File {filename} was created within organization {org}")
        return {"file_location": f"{URL}/{org}/{filename}", "result": 200}

    else:
        logger.error(f"Could not create file {filename} within org {org}")
        logger.error(f"Response from S3 {dumps(res, indent=4)}")
        raise Exception(f"Could not create organization {org}")


def delete_file(filename, org):
    s3 = _get_s3_resource()
    obj = s3.Object("fstatsfiles", f"{org}/{filename}")

    if not _check_file_exists(obj):
        logger.error(f"File {filename} within organization {org} does not "
                     f"exist")

        raise Exception(f"File {filename} does not exist within organization "
                        f"{org}")

    res = obj.delete()
    if 200 <= res["ResponseMetadata"]["HTTPStatusCode"] <= 299:
        logger.info(f"Delete file {filename} within organization {org} "
                    f"successful")
        logger.info(f"Response was {res}")
    return {"result": 200}


def _extract_key(furl):
    global URL
    try:
        return furl.split(URL+"/")[1].splot("/")
    except Exception as ex:
        logger.error("Caught exception while parsing the URL")
        logger.error(f"Exception was {ex}")
        raise


def get_file(url):
    s3 = _get_s3_resource()
    org, filename = _extract_key(url)
    obj = s3.Object(org, filename)

    if not _check_file_exists(obj):
        logger.error(f"File {filename} within organization {org} does not "
                     f"exist")

        raise Exception(f"File {filename} does not exist within organization "
                        f"{org}")

    res = obj.get()

    if 'Body' not in res:
        logger.error(f"'Body' key for file {filename} in organization {org} was"
                     f" not found in response from S3!")
        raise Exception(f"Internal error while trying to access {filename} "
                        f"within organizatio {org}")

    return res['Body']


def _get_s3_resource():
    global DEBUG
    if DEBUG:
        return resource('s3')
    else:
        return resource('s3')


if __name__ == "__main__":
    from base64 import b64encode
    DEBUG = False
    with open("/Users/adamalloul/mv_assets.py", "r") as f:
        filecontent = b64encode(f.read().encode("utf-8"))

    delete_file("mv_assets2.py", "org1")
    file_location = upload_file("mv_assets2.py", filecontent, "org1")
