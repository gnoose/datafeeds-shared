FROM selenium/standalone-chrome:3.141.59-20200525

RUN sudo apt-get update && sudo apt-get install -y python3-pip
RUN sudo mkdir -p /app/workdir
WORKDIR /app
COPY requirements.txt /app
RUN pip3 install -r requirements.txt

COPY datafeeds /app/datafeeds
COPY launch.py /app
RUN sudo chown -R seluser:seluser /app
