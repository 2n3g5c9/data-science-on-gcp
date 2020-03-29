#!/usr/bin/env bash
gcloud app create --region eu-west

git clone https://github.com/GoogleCloudPlatform/python-docs-samples
cd python-docs-samples/appengine/standard_python37/hello_world

gcloud app deploy --quiet --stop-previous-version
