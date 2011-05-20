#!/bin/sh

HOST="localhost"
PORT="5984"

curl -X DELETE http://$HOST:$PORT/couch_writes_test
curl -X PUT http://$HOST:$PORT/couch_writes_test
curl -X PUT http://$HOST:$PORT/_config/couchdb/delayed_commits -d '"false"'


./basho_bench couch_tests/couch_http_writes.config
