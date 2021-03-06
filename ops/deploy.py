#!/usr/bin/env python

############################
# Three options for use:
# 1) Default behavior:
#    find commit ID of HEAD on git on master branch
#    confirm this exists as docker tag on ECR
#    retag this as "deployed"
#    print error if the latest master commit ID doesn't exist on ECR
# 2) specify a specific commit ID
#    other steps same as above
#    to use this option, run "python retag.py --githash <COMMIT_ID>"
# 3) specify a specific branch
#    to use this option, run "python retag.py --branch <BRANCH_NAME>"
############################

import argparse
import logging
import sys

import boto3
from git import Repo

import config
from datafeeds.common.alert import post_slack_message
from datafeeds.config import DATAFEEDS_ROOT


DEPLOY_TAG = "deployed"

slack_channel = config.SLACK_CHANNEL

log = logging.getLogger(__name__)

ecr_client = boto3.client("ecr", region_name=config.AWS_REGION_NAME)


def get_commit_id(branch: str) -> str:
    repo = Repo(DATAFEEDS_ROOT)
    # assumes remote is named "origin"
    log.info("Pulling latest from remote")
    branch_head = getattr(repo.heads, branch)
    branch_head.checkout()
    repo.remotes.origin.pull()
    commit_id = branch_head.commit.hexsha
    log.info("Commit ID at head of %s: %s", branch_head, commit_id)
    return commit_id


def find_image_tag(docker_tag: str, repo: str = "datafeeds") -> bool:
    image_records = []

    # assume we're getting region and creds from environment
    try:
        response = ecr_client.list_images(repositoryName=repo)
        while True:
            image_records = image_records + response.get("imageIds", [])
            next_token = response.get("nextToken")
            if next_token is None:
                break
            response = ecr_client.list_images(repositoryName=repo, nextToken=next_token)
    except Exception:
        log.exception("Exception raised: failed to get list of images")
        raise

    for image in image_records:
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
        ecr_client.batch_delete_image(
            repositoryName=ecr_repo, imageIds=[{"imageTag": tag_to_remove}]
        )
    else:
        log.exception("No existing image found with tag: %s", tag_to_remove)


def retag_image(current_tag: str, new_tag: str, ecr_repo: str = "datafeeds") -> None:
    image_manifest = ecr_client.batch_get_image(
        repositoryName=ecr_repo, imageIds=[{"imageTag": current_tag}]
    )["images"][0]["imageManifest"]
    ecr_client.put_image(
        repositoryName="datafeeds", imageManifest=image_manifest, imageTag=new_tag
    )
    log.info("Tagged image with existing tag %s with %s tag", current_tag, new_tag)


######################################################


parser = argparse.ArgumentParser(description="Deploy a datafeeds image")
group = parser.add_mutually_exclusive_group(required=False)
group.add_argument(
    "--branch", type=str, help="the git branch to deploy", default="master"
)
group.add_argument("--githash", type=str, help="the git commit hash to deploy")


def main():
    args = parser.parse_args()

    if args.githash:
        how = "Git Hash: %s" % args.githash
        log.info("Attempting to retag the image with git hash %s.", args.githash)
        commit_id = args.githash
    else:
        how = "Branch: %s" % args.branch
        log.info("Attempting to deploy the image for branch %s.", args.branch)
        commit_id = get_commit_id(args.branch)

    post_slack_message("A datafeeds deploy has started. (%s)" % how, slack_channel)
    image_tag_exists = find_image_tag(commit_id)

    if image_tag_exists:
        try:
            remove_image_tag(DEPLOY_TAG)
            retag_image(commit_id, DEPLOY_TAG)
            log.info("Retagging image completed successfully for %s.", commit_id)
            post_slack_message(
                "Retagging image completed successfully for %s." % commit_id,
                slack_channel,
            )
        except Exception:  # noqa E722
            log.exception("Retagging image failed for %s", commit_id)
            post_slack_message(
                "Retagging image failed for %s" % commit_id, slack_channel, icon=":x:"
            )
            sys.exit(1)

    log.info("Done.")
    sys.exit(0)


if __name__ == "__main__":
    log.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s: %(message)s")
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    # log.addHandler(ch)
    # https://stackoverflow.com/a/7175288

    main()
