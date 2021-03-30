# Cloud9 (AWS browser IDE)

[Cloud9 User Guide](https://docs.aws.amazon.com/cloud9/latest/user-guide/welcome.html)

[Team setup for AWS Cloud9](https://docs.aws.amazon.com/cloud9/latest/user-guide/setup.html)

[VPC settings for AWS Cloud9 Development Environments](https://docs.aws.amazon.com/cloud9/latest/user-guide/vpc-settings.html)

This describes how to create a new Cloud9 environment for working with datafeeds-shared. To work
with an existing environment, see [cloud9_usage.md](cloud9_usage.md).

## Create environment

[Create an EC2 Environment with the console](https://docs.aws.amazon.com/cloud9/latest/user-guide/create-environment-main.html)

Create environment in dev AWS console: https://console.aws.amazon.com/cloud9/home

  - Environment type: Create a new Ec2 instance for environment (direct access)
  - Instance type: `m5.large`
  - Platform: Amazon Linux 2
  - Network (VPC): `energy | vpc-0bc1f71e8eabaa81d`
  - Subnet: `subnet-0accb782fe04c57d2 / energy-public-b`
  - Add tag Key = `name`, Value = `Cloud9 environment-name`

Add a rule for the newly created security group to access the databases:

  - Find the new security group on the [VPC / Security Groups page](https://console.aws.amazon.com/vpc/home?region=us-east-1#securityGroups:search=cloud;sort=desc:tag:Name)
  - On the [energy-pg](https://console.aws.amazon.com/vpc/home?region=us-east-1#SecurityGroup:groupId=sg-0e3a383e4d21ac849) security group, add an Inbound rule:
    - Type = Postgres SQL
    - Source = [new security group id]
    - Description = Access from Cloud9 *environment name*
  - On the [rds-urjanet](https://console.aws.amazon.com/vpc/home?region=us-east-1#securityGroups:group-name=rds-urjanet) security group, add an Inbound rule:
    - Type = MYSQL/Aurora
    - Source = [new security group id]
    - Description = Access from Cloud9 *environment name*

## Setup environment

Open environment from [AWS Cloud9](https://console.aws.amazon.com/cloud9/home?region=us-east-1)

Set up git to use the ssh key for the `gridium-datafeeds` user. This user has outside collaborator access
to the datafeeds-shared repo.

Find the gridium-datafeeds entry in LastPass. Copy the ssh key to `~/.ssh/id_rsa` and set permissions: `chmod 0600 ~/.ssh/id_rsa`. Add the key to the agent:

```
eval "$(ssh-agent -s)"
ssh-add ~/.ssh/id_rsa
```

Clone the datafeeds-shared repo: `git clone git@github.com:Gridium/datafeeds-shared.git`

Cut and paste `energy-dev-ops:/var/config-data/datafeeds/dev.env` into `~/environment/dev.env` in Cloud9.

Install pyenv and python 3.8.5 virtualenv:

```
yum install gcc zlib-devel bzip2 bzip2-devel readline-devel sqlite sqlite-devel openssl-devel tk-devel libffi-devel
curl https://pyenv.run | bash
pyenv install -v 3.8.5
pyenv virtualenv 3.8.5 datafeeds
pyenv activate datafeeds
```

Add these to `~/.bashrc` then `source ~/.bashrc`:

```
export PATH="/home/ec2-user/.pyenv/bin:$PATH"
eval "$(pyenv init -)"
eval "$(pyenv virtualenv-init -)"
export PYTHONPATH=/home/ec2-user/environment/datafeeds-shared
source /home/ec2-user/environment/dev.env
```

Install dependencies

```
cd datafeeds-shared
pip install -r requirements.txt
pip install -r dev-requirements.txt
docker pull selenium/standalone-chrome:3.141.59-20200525
```

The default EBS for Cloud9 is 10GB, which is not quite enough. Resize the volume to 20GB:

https://docs.aws.amazon.com/cloud9/latest/user-guide/move-environment.html#move-environment-resize


## Setup users

Create an AWS IAM user (if neeeded). Add user to `Cloud9User` group, which has `AWSCloud9User` role.

In environment, click Share in top right and invite IAM user with read/write permissions.
