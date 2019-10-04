FROM selenium/standalone-chrome:3.141.59

RUN sudo apt-get update && sudo apt-get install -y python3-pip
RUN sudo mkdir -p /app/workdir

COPY datafeeds /app/datafeeds
COPY launch.py /app
COPY requirements.txt /app

WORKDIR /app
RUN pip3 install -r requirements.txt
RUN sudo chown -R seluser:seluser /app
