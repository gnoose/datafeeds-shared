docker ps -a | grep chrome | awk '{ print $1 }' | xargs docker stop
docker ps -a | grep chrome | awk '{ print $1 }' | xargs docker rm
docker run -d -p 4444:4444 -p 8080:7900 --name=selenium_chrome  -v /dev/shm:/dev/shm \
  -v ~/environment/datafeeds-shared/workdir:/home/ec2-user/environment/datafeeds-shared/workdir \
  selenium/standalone-chrome:4.0.0-beta-3-20210426

