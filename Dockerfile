FROM selenium/standalone-chrome:3.141.59

RUN sudo apt-get update && sudo apt-get install -y python3-pip
RUN sudo mkdir /app && sudo chown seluser:seluser /app

COPY . /app
WORKDIR /app


RUN pip3 install -r requirements.txt
