#!/usr/bin/env bash
echo "indexing docs"

# update env variables in shell
source .env

# load documents and update index
python3 ./scripts/ingest_data.py