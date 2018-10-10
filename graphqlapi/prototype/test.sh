#!/bin/bash
if ! git diff-index --quiet HEAD ../../test/graphql_schema/schema_design.json
then
    echo "there is a difference in the schema's lets commit it back"
fi