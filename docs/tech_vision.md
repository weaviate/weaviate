# Technology Vision

> This document describes the technical vision of what Weaviate will be.
> To have an overview of what is implemented currently, look in the [Current Architecture](./tech_current_architecture.md) file.

## Introduction

Weaviate is a decentralized knowledge graph which aims to;
1. Provide scalable graph databases.
2. Provide easy access to the graph through RESTful API's and GraphQL end-points.
3. Allow the creation of cross Weaviate graphs over a P2P network.
4. Allow fuzzy traversing over multiple-graphs in a P2P network.
5. Uses a word vector based solution (_Contextionary_) to give linguistical context to words in the database.

> Note: for the implementation-roadmap see the [README.md](../README.md) file

## Architecture overview

The Weaviate server offers an API for users and uses a (graph) database to store the data.

A user can consume the Weaviate service through a set of RESTful end-points or through the GraphQL API.

Below you'll find an overview of the data model, the role of the contextionary, and the offered API's.

### Data model: Things, Actions, Keys

- Weaviate stores three types of data: Things, Actions and Keys.
- Keys are used both for authentication and authorization in Weaviate.
- Owners of a key can create more keys; the new key points to the parent key that is used to create the key.
- Permissions (read, write, delete, execute) are linked to a key.
- Each piece of data is an instance of a Class. Each class is either a Thing or an Action.
- A Class has a name, optional keywords, and properties.
- A Class can have (primitive) properties (e.g., numbers, strings) and references to other Class instances.
- Each piece of data (e.g., Things & Actions) is associated with a Key.
- A Thing describes a (physical) object; an Action describes a change.

### Contextionary

The contextionary is an abstraction that allows us to work with trained Word Vectors easily. It consists of an Approximate k-Nearest Neighbours memory mapped data structure and a memory mapped index for words. This allows us to find the Vector of a word quickly. From a vector, we can find words that are close.

The contextionary will be used to query remote data sources. It is used to ensure that the names of thing and action classes and their properties can be encoded to a vector.

### API's

Weaviate offers a REST API, with
- Endpoints to insert/modify the data and define the schema.
- A GraphQL endpoint to query the data.

For details, check the automatically generated documentation of the RestAPI and GraphQL API's in
[`docs/generated`](./generated/)

## Database schema

The database has a strongly typed schema. There are two types of classes; Things and Actions.

Although the schemas are identical from the database schema perspective, they are separated to make it easier for end users to interact with the service (i.e., actions are verbs, things are nouns)

A class has a name, optional keywords to clarify the semantic meaning of the class, some meta-data, and properties.

A property is either a key-(primitive) value pair or a key-reference pair.

Weaviate supports the following primitive types:
- `string`
- `int` (stored as an int64)
- `number` (stored as a double)
- `boolean`
- `date`

A reference can point to one or more types of classes. These classes can be both Things and Actions.
Note: In future releases, we will extend this with a remote reference.

### Modifying the schema

Weaviate provides several REST endpoints to modify the schema.

On both the class and property level, we support these operations:
- Create
- Update name and/or keywords
- Drop

Weaviate explicitly does not support changing the type of properties.
