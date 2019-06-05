#!/bin/bash

search_term="${1:required}"

query="{ Local { Explore { Concepts(values:[\\\"$search_term\\\"]) { className beacon } } } }"
body="{\"query\":\"$query\"}"
endpoint=localhost:8080/weaviate/v1/graphql

curl -s -H 'Content-Type: application/json' -d "$body" $endpoint | jq -r .data.Local.Explore.Concepts[].beacon | while read -r url; do
res=$(echo "$url" | sed 's#weaviate://localhost#http://localhost:8080/weaviate/v1#g' | xargs curl -s)
echo -en "$res" | jq -r .class | tr -d '\n'
echo -en "\t"
echo -en "$res" | jq -r .schema.name

done;
