#!/usr/bin/env bash
set -euo pipefail

# This scripts starts a Weaviate prototype server with the test scheme and waits until weaviate is up and running.
# After starting the server it creates a new introspection schema
# if changed, it will be committed back.

# VARIABLES
MAX_WAIT_SECONDS=60
ALREADY_WAITING=0
NODE_PORT=8081

# go to the prototype dir
cd graphqlapi/prototype

# install deps
npm install

# run nodemon in the background - sleep to wait for startup of node server
nohup node prototype-server.js &

# poll service to check if live
while true; do
  if curl -s http://localhost:$NODE_PORT > /dev/null; then
    break
  else
    if [ $? -eq 7 ]; then
      echo "Weaviate prototype is not up yet. (waited for ${ALREADY_WAITING}s)"
      if [ $ALREADY_WAITING -gt $MAX_WAIT_SECONDS ]; then
        echo "Weaviate prototype did not start up in $MAX_WAIT_SECONDS."
        exit 1
      else
        sleep 2
        let ALREADY_WAITING=$ALREADY_WAITING+2
      fi
    fi
  fi
done

# W00T!
echo "Weaviate prototype is up and running!"

# Request the introspection
# note: introspection used: https://github.com/creativesoftwarefdn/weaviate/blob/develop/docs/graphql_introspection.md
curl -d '{"query":"{\n  __schema {\n    queryType {\n      name\n    }\n    mutationType {\n      name\n    }\n    subscriptionType {\n      name\n    }\n    types {\n      ...FullType\n    }\n    directives {\n      name\n      description\n      locations\n      args {\n        ...InputValue\n      }\n    }\n  }\n}\n\nfragment FullType on __Type {\n  kind\n  name\n  description\n  fields(includeDeprecated: true) {\n    name\n    description\n    args {\n      ...InputValue\n    }\n    type {\n      ...TypeRef\n    }\n    isDeprecated\n    deprecationReason\n  }\n  inputFields {\n    ...InputValue\n  }\n  interfaces {\n    ...TypeRef\n  }\n  enumValues(includeDeprecated: true) {\n    name\n    description\n    isDeprecated\n    deprecationReason\n  }\n  possibleTypes {\n    ...TypeRef\n  }\n}\n\nfragment InputValue on __InputValue {\n  name\n  description\n  type {\n    ...TypeRef\n  }\n  defaultValue\n}\n\nfragment TypeRef on __Type {\n  kind\n  name\n  ofType {\n    kind\n    name\n    ofType {\n      kind\n      name\n      ofType {\n        kind\n        name\n        ofType {\n          kind\n          name\n          ofType {\n            kind\n            name\n            ofType {\n              kind\n              name\n              ofType {\n                kind\n                name\n              }\n            }\n          }\n        }\n      }\n    }\n  }\n}\n","variables":null,"operationName":null}' -H "Content-Type: application/json" -X POST http://localhost:$NODE_PORT/graphql | jq '.' > ../../test/graphql_schema/schema_design.json

# done, kill the process
kill $!

# back to original dir
cd ../..

# check if there is a difference in the schema of the prototype and the test schema
if ! git diff-index --quiet HEAD test/graphql_schema/schema_design.json
then
  # Set git config
  git config --global user.email "travis@travis-ci.org"
  git config --global user.name "Travis CI 🤖"

  # Add schema file & commit & push
  git config credential.helper "store --file=.git/credentials"
  echo "https://${GH_TOKEN}:@github.com" > .git/credentials
  git add test/graphql_schema/schema_design.json
  git commit -m "🤖 bleep bloop - auto updated the graphql schema"
  git push origin HEAD:${TRAVIS_PULL_REQUEST_BRANCH:-$TRAVIS_BRANCH}

  # some log messaging
  echo "there is a difference in the schema's lets commit it back, it will trigger a new Travis build"
  echo "need to stop this build :-("

  # stop
  exit 1

fi

echo "Schema validating from Weaviate GraphQL NodeJS prototype = done"