FROM python:3.11-alpine
LABEL authors="PUT_YOUR_NAME_HERE"

WORKDIR app/

ADD ./metric_watch ./metric_watch
ADD ./metrics_hierarchy.yml ./
ADD ./templates ./templates
COPY ./main.py ./
COPY ./requirements.txt ./

RUN pip3 install -r requirements.txt
