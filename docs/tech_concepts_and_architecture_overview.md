# Technical Concepts & Architecture Overview

<pre>
NOTE: This describes the desired situation at for the milestone where pilot users will be able
      to onboard their data.
</pre>

## Introduction
Weaviate is a middlware that makes using a scaleable graph databases easy.
In the near future, support will be added to enrich user queries with data from external data sources.

## Architecture overview
The Weaviate server offers an API for users, and uses a (graph) database to store the data.

The rest of this section we describe the data model, the role of the contextionary, and the offered API's.

### Data model: Things, Actions, Keys
- Weaviate stores three types of data: Things, Actions and Keys.
- Keys are used both for authentication and authorization in Weaviate.
- Owners of a key can create more keys; the new key points to the parent key that is used to create the key.
- Permissions (read, write, delete, execute) are linked to a key.
- Each piece of data is an instance of a Class. Each class is either a Thing or an Action.
- A Class has a name, optional keywords, and properties.
- A Class can have (primitive) properties (e.g. numbers, strings) and references to other Class instances.
- Each piece of data (e.g. Things & Actions) is associated with a Key.
- A Thing describes a (physical) object, an Action describes a change.

### Contextionary
The contextionary is an abstraction that allows us to easily work with trained Word Vectors.
It consists of a Approximate k-Nearest Neighbours memory mapped data structure and a memory mapped
index for words. This allows us to quickly find the Vector of a word. From a vector, we can find
words that are close.

This will be used in the next milestone to query remote data sources. In this milestone,
it is used to ensure that the names of thing and action classes and their properties can be
encoded to a vector.

### API's
Weaviate offers a REST API, with
- Endpoints to insert/modify the data and define the schema.
- A GraphQL endpoint to query the data.

For details, check the automatically generated documentation of the RestAPI and GraphQL API's in
[`docs/generated`](./generated/)

## Database schema
The database has a strongly typed schema. There are two types of classes; Things and Actions.

From the perspective of the Database schema, these are identical, so we'll only dicuss the schema
of this general class.

A class has a name, optional keywords to clarify the semantic meaning of the class, some meta-data and properties.

A property is either a key-(primitive) value pair or a key-reference pair.

Weaviate supports the following primitive types:
- `string`
- `int` (stored as an int64)
- `number` (stored as a double)
- `boolean`
- `date`

A reference can point to one or more types of classes. These classes can be both Things and Actions.
Note: In future releases we will extend this with a remote reference.

### Modifying the schema
Weaviate provides several REST endpoints to modify the schema.

On both the class and property level, we support these operations:
- Create
- Update name and/or keywords
- Drop

Weaviate explicitly does not support changing the type of properties.
