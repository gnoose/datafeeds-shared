#!/usr/bin/env python

############################
# Two options for use:
# 1) Default behavior:
#     find commit ID of HEAD on git on master branch
#    confirm this exists as docker tag on ECR
#    retag this as "deployed"
#    print error if the latest master commit ID doesn't exist on ECR
# 2) specify a specific commit ID
#     other steps same as above
#     to use this option, run "python retag.py <COMMIT_ID>"
############################

from git import Repo
import boto3
import config
import logging
import slack
import sys
import slack.chat

DEPLOY_TAG = "deployed"
GIT_BRANCH = "master"

log = logging.getLogger(__name__)

ecr_client = boto3.client("ecr")


def post_message(message, channel, icon=':mega:'):
    if not config.SLACK_TOKEN:
        return

    try:
        slack.api_token = config.SLACK_TOKEN
        slack.chat.post_message(channel, message, username='webapps', icon_emoji=icon)   # pylint: disable=no-member
    except:
        log.exception('Failed to post error message to slack. Channel: %s, Message: %s', channel, message)


def get_commit_id(branch: str) -> str:
    repo = Repo()
    # assumes remote is named "origin"
    log.info("Fetching latest from remote")
    repo.remotes.origin.fetch()
    if len(sys.argv) == 2:
        log.info("Using %s for commit ID", sys.argv[1])
        return sys.argv[1]
    branch_head = getattr(repo.heads, branch)
    id = branch_head.commit.hexsha
    log.info("Commit ID at head of %s: %s", branch_head, id)
    return id


def find_image_tag(docker_tag: str, repo: str = "datafeeds") -> bool:
    # assume we're getting region and creds from environment
    try:
        response = ecr_client.list_images(repositoryName=repo)
    except:
        log.exception("Exception raised: failed to get list of images")
        raise
    for image in response["imageIds"]:
        if "imageTag" in image:
            if docker_tag == image["imageTag"]:
                log.info("Found image tag: %s", docker_tag)
                return True
    log.exception("Did not find tag %s in the ECR repository", docker_tag)
    return False


def remove_image_tag(tag_to_remove: str, ecr_repo: str = "datafeeds") -> None:
    # note: the image is only removed if the only tag is removed
    tag_to_remove_exists = find_image_tag(tag_to_remove)
    if tag_to_remove_exists:
        ecr_client.batch_delete_image(repositoryName=ecr_repo, imageIds=[{'imageTag': tag_to_remove}])
    else:
        log.exception("No existing image found with tag: %s", tag_to_remove)


def retag_image(current_tag: str, new_tag: str, ecr_repo: str = "datafeeds") -> None:
    image_manifest = ecr_client.batch_get_image(repositoryName=ecr_repo, imageIds=[{"imageTag": current_tag}])["images"][0]["imageManifest"]
    ecr_client.put_image(repositoryName="datafeeds", imageManifest=image_manifest, imageTag=new_tag)
    log.info("Tagged image with existing tag %s with %s tag", current_tag, new_tag)


def main():
    commit_id = get_commit_id(GIT_BRANCH)
    image_tag_exists = find_image_tag(commit_id)

    if image_tag_exists:
        try:
            remove_image_tag(DEPLOY_TAG)
            retag_image(commit_id, DEPLOY_TAG)
            log.info("Retagging image completed successfully for %s.", commit_id)
            post_message("Retagging image completed successfully for %s." % commit_id, "#ops")

        except:
            log.exception("Retagging image failed for %s", commit_id)
            post_message("Retagging image failed for %s" % commit_id, '#ops', icon=":x:")
            sys.exit(1)

    log.info('Done.')
    sys.exit(0)


if __name__ == "__main__":
    log.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s: %(message)s")
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    log.addHandler(ch)

    main()
