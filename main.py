import logging
from sys import stdout
from json import dumps
from modules.organization_utils import create_organization, delete_organization
from modules.file_utils import upload_file, delete_file, get_file
from modules.reporting import report_usage
from modules.stats import view_stats

# Logging
logging.basicConfig(stream=stdout, format='%(asctime)s %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def dispatch_request(req):
    if req['action_type'] == 'create_organization':
        logger.info(f"Creating new organization {req['organization_name']}")
        return create_organization(req['organization_name'])

    elif req['action_type'] == 'delete_organization':
        logger.info(f"Deleting organization {req['organization_name']}")
        return delete_organization(req['organization_name'])

    elif req['action_type'] == 'upload':
        logger.info(f"Uploading new file with name {req['content']}, within "
                    f"organization {req['organization_name']}")
        return upload_file(req['filename'], req['content'],
                           req['organization_name'])

    elif req['action_type'] == "get_file":
        logger.info(f"File {req['filename']} within organization "
                    f"{req['organization_name']} was requested")
        return get_file(req['filename'])

    elif req['action_type'] == 'delete_file':
        logger.info(f"Deleting file {req['filename']}, within organization "
                    f"{req['organization_name']}")
        return delete_file(req['filename'], req['organization_name'])

    elif req['action_type'] == 'get_stats':
        logger.info(f"Generating stats for file {req['filename']}, within "
                    f"organization {req['organization_name']}")
        return view_stats(req)

    else:
        logger.error(
            f"Unknown value {req['action_tyoe']} for 'action_type' key")
        raise Exception("Malformed request")


def write_stats():
    pass


def main(event, context):
    """
    Expected Payload
        {
            "request_tstamp": 1534601967000,
            "action_type": "upload" | "get_stats" | "create_organization" |
                            "delete_file" | "get_file" | "delete_organization",
            "content": "base64",
            "filename": 'fname'
            "channels": "channel IDs to post to",
            "users": "user IDs to post to",
            ---- For stats purposes ---
            "username": "adam",
            "userID": "user slackID",
            "organization_name": "tripaneer",
            "organization_id": "organization slackID",
        }
    :return:
    """

    logger.info("Request received")
    if 'action_type' not in event['body-json']:
        logger.error("action_type not found in body - raise Exception")
        logger.error(f"Full request was {dumps(event, indent=4)}")
        raise Exception("Request malformed")

    try:
        return dispatch_request(event['body-json'])

    except Exception:
        logger.error(f"Full request was {dumps(event, indent=4)}")
        raise

    finally:
        report_usage(event)

if __name__ == "__main__" or True:
    from time import time

    # Create Organization
    req = {
        "body-json": {
            "request_timestamp": int(time()),
            "action_type": "create_organization",
            "organization_name": "org1"}
    }
    main(req, "")

    # Upload file 1
    from base64 import b64encode
    with open("/Users/adamalloul/mv_assets.py", "r") as f:
        filecontent = b64encode(f.read().encode("utf-8"))
    req = {
        "body-json": {
            "request_timestamp": int(time()),
            "action_type": "upload",
            "organization_name": "org1",
            "filename": "mv_assets.py",
            "content": filecontent
        }
    }
    main(req, "")

    # Upload file 2
    req = {
        "body-json": {
            "request_timestamp": int(time()),
            "action_type": "upload",
            "organization_name": "org1",
            "filename": "mv_assets2.py",
            "content": filecontent
        }
    }
    main(req, "")

    # Delete file 2
    req = {
        "body-json": {
            "request_timestamp": int(time()),
            "action_type": "delete_file",
            "organization_name": "org1",
            "filename": "mv_assets2.py",
        }
    }
    main(req, "")

    # Delete Organization
    req = {
        "body-json": {
            "request_timestamp": int(time()),
            "action_type": "delete_organization",
            "organization_name": "org1",
        }
    }
    main(req, "")
