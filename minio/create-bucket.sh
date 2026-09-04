#!/bin/sh

mc alias set local http://minio:9000 minio minio123456

mc mb --ignore-existing local/delta-data

mc anonymous set private local/delta-data

echo "Bucket delta-data created."
