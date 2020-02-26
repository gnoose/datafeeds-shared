# Working with contractors

Contractors should be added as outside collaborators to the
[datafeeds-shared](https://github.com/Gridium/datafeeds-shared) repo, not this one.
This prevents access to the CircleCI configuration that we don't want to share.

## Setup for Gridium employees

Clone the `datafeeds-shared` repo

    git clone https://github.com/Gridium/datafeeds-shared.git

Add `datafeeds` as a remote:

    cd datafeeds-shared
    git remote add upstream https://github.com/gridium/datafeeds

Before starting a new job, sync `datafeeds` changes to `datafeeds-shared`. From `datafeeds-shared`:

    git checkout master
    git pull --rebase # get latest datafeeds-shared
    git fetch upstream  # get latest datafeeds
    git merge upstream/master # merge datafeeds master into datafeeds-shared master
    git push origin master # push to datafeeds-shared master

Create a pull request in `datafeeds-shared` for the contractor;
copy the contents of [porting_urjanet.md](porting_urjanet.md) into the PR description.

Assign the job to a contractor. They should push their changes to a branch in the `datafeeds-shared` repo.

When the PR is ready to merge, sync `datafeeds-shared` changes to `datafeeds`:

    git checkout contractor-branch
    git pull --rebase # get latest work
    git pull --rebase upstream master # get latest datafeeds
    git merge upstream/master # merge datafeeds-shared work into datafeeds master
    git push upstream master # push to datafeeds master


## Setup for contractors

Contractors should work in the `datafeeds-shared` repo.

See directions in [porting_urjanet.md](porting_urjanet.md).
