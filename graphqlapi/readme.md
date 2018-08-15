 GraphQL endpoint generation for Weaviate
The point of external access is in `graphql_schema.go` (`CreateSchema()`). The schema is assembled in its entirety in `assembleFullSchema()` in `build_schema.go`.

## The schema
The schema is based on instances of the `graphql.Object` class. These `graphql.Object` objects have a property that contains a `graphql.Fields` object, which is a `map[string]graphql.Field`. This map contains one or more `graphql.Field` objects. These `Field` objects contain a `Scalar` type or another `graphql.Object` as value. 

## The code
The code is divided in functions that return a single `graphql.Object` and its properties (including the `Fields` property, which can contain `Field` objects with `graphql.Object` types). The aim of this is to make the code easier to read. The code for schema generation is divided in a statically generated part, a dynamically generated part and filter functionality.

Both `Filters` and `Things`/`Actions` can contain cyclical relationships; these cases are solved by using `thunks`.

#### The static schema part is generated in: 
- `build_schema.go` 

#### The dynamic schema parts are generated in: 
- `dynamic_generation_converted_fetch.go`
- `dynamic_generation_meta_fetch.go`

#### The filters are generated in:
- `filters.go`
