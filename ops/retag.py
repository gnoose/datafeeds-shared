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
import base64
import boto3
import sys

DEPLOY_TAG='deployed'
GIT_BRANCH='master'

ecr_client = boto3.client('ecr')

def get_commit_id(branch):
    repo = Repo()
    # assumes remote is named 'origin'
    print("Fetching latest from remote")
    repo.remotes.origin.fetch()
    if len(sys.argv) == 2:
        print(f'Using {sys.argv[1]} for commit ID')
        return sys.argv[1]
    branch_head = getattr(repo.heads, branch)
    id = branch_head.commit.hexsha
    print(f'Commit ID at head of {branch_head}: {id}')
    return id

def find_image_tag(docker_tag, repo='datafeeds'):
    # assume we're getting region and creds from environment
    try:
        response = ecr_client.list_images(repositoryName=repo)
    except:
        print("Exception raised: failed to get list of images")
        raise
    for image in response['imageIds']:
        if 'imageTag' in image:
            if docker_tag == image['imageTag']:
                print(f'Found image tag: {docker_tag}') 
                return True
    print(f'Did not find the {docker_tag} tag in the ECR repository')            
    return False

def remove_image_tag(tag_to_remove, ecr_repo='datafeeds'):
    # note: the image is only removed if the only tag is removed
    tag_to_remove_exists = find_image_tag(tag_to_remove)
    if tag_to_remove_exists:
        ecr_client.batch_delete_image(repositoryName=ecr_repo, imageIds=[{'imageTag': tag_to_remove}]) 
    else:
        print(f'No existing image found with tag: {tag_to_remove}')

def retag_image(current_tag, new_tag, ecr_repo='datafeeds'):
    image_manifest = ecr_client.batch_get_image(repositoryName=ecr_repo, imageIds=[{'imageTag': current_tag}])['images'][0]['imageManifest']
    ecr_client.put_image(repositoryName='datafeeds', imageManifest=image_manifest, imageTag=new_tag)


commit_id = get_commit_id(GIT_BRANCH)
image_tag_exists = find_image_tag(commit_id)

if image_tag_exists:
    remove_image_tag(DEPLOY_TAG)
    retag_image(commit_id, DEPLOY_TAG)

