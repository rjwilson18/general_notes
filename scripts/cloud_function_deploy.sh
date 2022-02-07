#!/bin/bash

FUNCTION_NAME=pgp_encryption
RUNTIME=python39
BUCKET=BUCKET_NAME
TRIGGER=google.storage.object.finalize

gcloud functions deploy $FUNCTION_NAME \
--runtime $RUNTIME \
--trigger-resource $BUCKET \
--trigger-event $TRIGGER
