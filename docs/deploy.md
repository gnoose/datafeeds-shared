# Deploying a datafeeds image to production

This document describes how to select a particular docker image for production use.

Datafeeds images are hosted in AWS ECR. The system is designed to use a docker image in the ECR with the "deployed" tag. Images are built in CircleCI and pushed to the dev and prod ECR's after passing tests. They are tagged with the associated git commit ID.

Each environment has a handful of configuration variables and python dependencies that are needed in order for the deploy automation to work. Before you start your deployment, activate the virtual environment in the root of the project folder, and run `source deploy.env` inside the ops folder to set the appropriate environment variables.

To add the deployed tag to an image run the `ops/deploy.py` script. There are two ways to run the script. The default method has no additional arguments and will look for the latest commit to master:

```
source venv/bin/activate
export PYTHONPATH=$(pwd)
source ops/deploy.env
python ops/deploy.py
```

A deploy can also be made by commit ID or branch can also be provided:
```
python ops/deploy.py --githash 8af19fe8e797caf65d1090e98c97c0f9eb58498f
python ops/deploy.py --branch my-scraper-branch
```
Note: If you want to be able deploy by branch, it's best to limit your branch's name to alphanumeric characters, underscores, and dashes (avoid special characters like slashes and colons, which docker uses as delimeters).

The script exits if it doesn't find the commit ID/tag in the ECR. If it does find the tag, it moves the "deployed" tag to the new image.

Note that in AWS there is a lifecycle policy limiting the total number of images to 250. 
