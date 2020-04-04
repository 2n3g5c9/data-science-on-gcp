#!/usr/bin/env bash

gcloud sql instances patch flights \
    --authorized-networks "$(curl -s https://ipinfo.io/ip)/32"
