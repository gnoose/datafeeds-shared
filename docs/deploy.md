# Deploying a datafeeds image to production

This document describes how to select a particular docker image for production use. 

Datafeeds images are hosted in AWS ECR. The system is designed to use a docker image in the ECR with the "deployed" tag. Images are built in CircleCI and pushed to the dev and prod ECR's after passing tests. They are tagged with the associated git commit ID. 

To add the deployed tag to an image run the `ops/deploy.py` script. There are two ways to run the script. The default method has no additional arguments and will look for the latest commit to master:

```
python ops/deploy.py
```

A specific commit ID can also be provided:
```
python ops/deploy.py 8af19fe8e797caf65d1090e98c97c0f9eb58498f
```

The script exits if it doesn't find the commit ID/tag in the ECR. If it does find the tag, it moves the "deployed" tag to the new image.  
