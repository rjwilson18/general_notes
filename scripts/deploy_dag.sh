#!/bin/bash

PROJECT_ID=project
TIER=dev
LOCATION=us-east4
SOURCE=pgp_encrypt_dag.py
DAG_FOLDER=DAGs

gcloud composer environments storage dags import \
    --environment $PROJECT_ID$TIER \
    --location $LOCATION \
    --source $SOURCE \
    --destination $DAG_FOLDER
