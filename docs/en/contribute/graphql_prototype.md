# GraphQL Prototype

> The GQL prototype is used to design and develop an UX friendly graphql API. It is standalone from Weaviate but the interface is used within Weaviate.

## Usage

To run the prototype: 
- Install `npm install -g nodemon`
- Install packages: `npm install`
- Run: `nodemon prototype-server.js demo_schema` or `nodemon prototype-server.js test_schema` if you want to run the prototype with the test schema.
The GraphQL API prototype will be running at `http://localhost:8081/`. To expore, you can use GraphiQL (an in-browser IDE for exploring GraphQL APIs), which can be found at `http://localhost:8081/graphql`.

To run the CLI tool:
- Install `npm install -g graphql-cli`
- `cd` into a new dir
- `graphql init` (schema for this prototype = `http://localhost:8081/graphql`)

To get the full graphql schema by the CLI tool:
- Run: `graphql get-schema`
The result will be in `./schema.graphql`

To get the full schema definitions in markdown by a graphql introspection query:
- Install `npm install -g graphql-markdown`
- Run `graphql-markdown ./path/to/schema.graphql > schema.md`
