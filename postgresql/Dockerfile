FROM python:3.9.1

# We need to install wget to download the csv file
RUN apt-get install wget
# psycopg2 is a postgres db adapter for python: sqlalchemy needs it
RUN pip install pandas sqlalchemy psycopg2-binary pyarrow

WORKDIR /app
COPY ingest_data.py ingest_data.py 

ENTRYPOINT [ "python", "ingest_data.py" ]

# docker run -it \
#    -e POSTGRES_USER="root" \
#    -e POSTGRES_PASSWORD="root" \
#    -e POSTGRES_DB="ny_taxi" \
#    -v docker_sql_dtc_postgres_volume_local:/var/lib/postgresql/data \
#    -p 5432:5432 \
#    postgres:13

