#!/usr/bin/env bash

set -euo pipefail

GRAFEAS_URL="http://localhost:8080/v1beta1"

cd "$(dirname "$0")"

echo "dc down"
docker-compose down

git checkout ../../mappings/occurrences.json

echo "start es and grafeas-current"
docker-compose up -d elasticsearch grafeas-current
echo "dc up finished"

while [[ "$(curl -s -o /dev/null -w '%{http_code}' "$GRAFEAS_URL/projects")" != "200" ]]; do
  echo "Waiting for Grafeas server to start"
  sleep 5
done

echo "making project"
curl --url "$GRAFEAS_URL/projects" \
  -H 'Content-Type: application/json' \
  -d '{"name": "projects/rode"}'

echo "making occurrence"
curl --url "$GRAFEAS_URL/projects/rode/occurrences" \
  -H 'Content-Type: application/json' \
  -d '{"name":"","resource":{"name":"","uri":"git://github.com/rode/collector-harbor@123abcde"},"note_name":"projects/provider_example/notes/exampleBuildNote","kind":"BUILD","build":{"provenance":{"id":"build identifier","project_id":"some project identifier","commands":[],"built_artifacts":[{"checksum":"123456","id":"harbor.prod.liatr.io/library/alpine@sha256:123456","names":["harbor.prod.liatr.io/library/alpine:latest","harbor.prod.liatr.io/library/alpine:v1.2.3"]},{"checksum":"123456","id":"harbor.dev.liatr.io/library/alpine@sha256:123456","names":["harbor.dev.liatr.io/library/alpine:latest","harbor.dev.liatr.io/library/alpine:v1.2.3"]}],"create_time":"2020-03-12T14:01:39.728983Z","start_time":"2020-03-12T14:02:39.728983Z","end_time":"2020-03-12T14:03:39.728983Z","creator":"user initiating the build","logs_uri":"location of build logs","source_provenance":{"artifact_storage_source_uri":"input binary artifacts from this build","context":{"git":{"url":"the git repo url","revision_id":"git commit hash"},"labels":{}}},"trigger_id":"triggered by code commit 123","builder_version":"some version of the builder"},"provenance_bytes":"Z3JhZmVhcw=="}}'

echo "Stopping grafeas-current"
docker-compose stop grafeas-current

cp occurrences.json ../../mappings

docker-compose up --build grafeas-next
