FROM python:3.9

WORKDIR /app
COPY taxi_data_pipeline.py taxi_data_pipeline.py
COPY requirements.txt requirements.txt
COPY taxi_zone_lookup.csv taxi_zone_lookup.csv

RUN apt-get install wget
RUN pip install -r requirements.txt

ENTRYPOINT ["python","taxi_data_pipeline.py"]