These tests require the fixtures in ./fixtures to be loaded:

```
go run ./tools/schema_loader -action-schema test/acceptance/graphql_resolvers_local/fixtures/actions_schema.json -thing-schema test/acceptance/graphql_resolvers_local/fixtures/things_schema.json 
go run ./tools/fixture_importer/ -fixture-file test/acceptance/graphql_resolvers_local/fixtures/data.json
```
