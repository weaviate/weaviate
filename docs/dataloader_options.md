# Dataloader implementation options
This doc contains our options for implementing a [dataloader](https://github.com/graph-gophers/dataloader). It has been created as a result of [#480](https://github.com/creativesoftwarefdn/weaviate/issues/480).

## Background
Our dataloader implementation exists as middleware between the GraphQL endpoint's resolvers and the database connector. It is written as a connector, implementing the [BaseConnector](https://github.com/creativesoftwarefdn/weaviate/blob/d70782a6619315b85518f7e79791fe70ed4698dd/connectors/database_connector.go#L30) interface.

## Implementation options

![getgraph options dataloader](https://user-images.githubusercontent.com/9214481/45943227-d19ddd00-bfe5-11e8-87a1-bfbeaa6b7262.png)

We are going with @etiennedi's [proposal for Option C](https://github.com/creativesoftwarefdn/weaviate/pull/491). The GetGraph() function creates a loss of information that it then needs to solve by reinterpreting the resolvers, it should therefore be omitted.

