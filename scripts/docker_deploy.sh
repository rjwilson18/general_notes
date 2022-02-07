#!/bin/bash

PROJECT_ID=project_id
ENVIRONMENT='dev'
VERSION='latest'
IMAGE_NAME=pgp_encryption
GCR_PATH=gcr.io/$PROJECT_ID$ENVIRONMENT/$IMAGE_NAME:$VERSION

docker build -t $IMAGE_NAME .
#docker run -t $IMAGE_NAME
docker tag $IMAGE_NAME $GCR_PATH
docker push $GCR_PATH
