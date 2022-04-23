#!/bin/sh

echo "Waiting for db to start..."
while ! nc -z db 5432; do
  echo "Waiting..."
  sleep 1
done

echo "db started"

cd src || ! echo "src not found"
python main.py
