FROM python:3.10.0a7-slim-buster

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

WORKDIR /usr/app

# install psycopg2 dependencies
RUN apt-get update  \
    && apt-get install -y build-essential libpq-dev netcat  \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY zno_data ./zno_data

COPY src ./src

COPY docker-entrypoint.sh .

ENTRYPOINT ["./docker-entrypoint.sh"]