# Weaviate

_Decentralised Semantic Knowledge Graph_

| Branch   | Status        |
| -------- |:-------------:|
| Master   | [![Build Status](https://api.travis-ci.org/creativesoftwarefdn/weaviate.svg?branch=master)](https://travis-ci.org/creativesoftwarefdn/weaviate/branches)
| Develop  | [![Build Status](https://api.travis-ci.org/creativesoftwarefdn/weaviate.svg?branch=develop)](https://travis-ci.org/creativesoftwarefdn/weaviate/branches)

*Important Note:
Weaviate is not fully production ready yet. Follow this repo or sign up for the [mailing list](http://eepurl.com/bRsMir) to stay informed about the progress.*

## Index

- [Documentation](#documentation)
	- [Questions](#questions)
- [Roadmap](#roadmap)
- [Concept](#concept)
	- [Why Use Weaviate?](#why-use-weaviate)
- [Architecture](#architecture)
	- [Design Principles](#design-principles)
	- [Overview](#overview)
	- [Database Connector & Graph Interface](#database-connector--graph-interface)
	- [Ontology](#ontology)
	- [Peer to Peer (P2P) network](#p2p-network)
- [Usage](#usage)
- [Roadmap](#roadmap)

## Documentation

[Full documentation is available here](https://www.semi.network/knowledge-base/implement/weaviate/).

*Follow this repo or sign up for the [mailing list](http://eepurl.com/bRsMir) to stay informed about the progress.*

#### Questions

- General questions: [Stackoverflow.com](https://stackoverflow.com/questions/tagged/weaviate).
- Issues: [Github](https://github.com/creativesoftwarefdn/weaviate/issues).
- Commercial use: [hello@semi.network](mailto:hello@semi.network).

## Roadmap

Based on the technology & concept summary which you can find [here](https://www.semi.network/knowledge-base/learn/technology-summary/).

- [x] [RESTful API](https://github.com/creativesoftwarefdn/weaviate/blob/develop/openapi-specs/schema.json)
- [x] Thing handling
- [x] Action handling
- [x] Key handling
- [x] Development DB connector
- [x] Cassandra connector
- [ ] Gremlin connector <= V0.10.0
- [ ] GraphQL implementation <= V0.11.0
- [x] [Contextionary](https://www.semi.network/knowledge-base/learn/technology-summary/#contextionary)
- [ ] P2P connection <= V0.12.0
- [ ] Miscellaneous <= V1.0.0

Additionally there are Docker containers and docker compose files available for running Weaviate.

## Concept

Weaviate is a [Decentralised](https://en.wikipedia.org/wiki/Peer-to-peer) [Semantic](https://en.wikipedia.org/wiki/Semantic_Web) [Knowledge Graph](https://hackernoon.com/wtf-is-a-knowledge-graph-a16603a1a25f). It allows for different nodes in a network to share knowledge from each node's individual graph.

What makes Weaviate unique;<br>
- Weaviate is plug & playable because it uses industry standards like [RESTful API](https://en.wikipedia.org/wiki/Representational_state_transfer)'s and [GraphQL](https://en.wikipedia.org/wiki/GraphQL).
- Every Weaviate can have a 100% unique ontology.
- [Can be connected](#user-content-database-connector--graph-interface) to any database of choice to solve specific use cases ([consistency, availability and/or partition tolerance wise](https://en.wikipedia.org/wiki/CAP_theorem)).
- Based on [word embedings](https://en.wikipedia.org/wiki/Word_embedding) to give context to [M2M](https://en.wikipedia.org/wiki/Machine_to_machine) communication over the network.

[More abstract concept description](https://bob.wtf/semantic-internet-of-things-42811e1ca7a7).

### Why Use Weaviate?

Weaviate solves the problem of relating seemingly different datasets to each other. It can be used to compare and describe data ranging from finance 🏦 to car manufacturing 🚗🏭, from zoos 🐘 to space stations 🚀 and from traditional datasets 📊 to Internet of Thing 📱 devices.

Besides doing that inside a single Weaviate instance. You can also create a peer-to-peer network of Weaviates that use word embeddings to communicate with each other in a unique way.

Want to learn more about large semantic networks or want to create a network for your business? Visit [www.semi.network](https://www.semi.network).

## Architecture

_The architecture of Weaviate is based on a core set of design principles which consists of the graph, the P2P network, and the word embeddings._

### Design Principles

Weaviate is build with the following axioms in mind;

1. Proven standards (i.e., RESTful, http(s) and GraphQL) should be sufficient.
2. Every Weaviate can have a unique ontology for both Things and Actions.
3. Every Weaviate can have a unique database underlying it.
4. Every Weaviate has a word embedded vector that gives context to questions posed over the network.
5. Every Weaviate can communicate vector coordinates over a https based P2P network.

### Overview

![Architectural overview](https://raw.githubusercontent.com/creativesoftwarefdn/weaviate/develop/assets/img/arch_overview.jpg "Architectural overview")

The above diagram contains a bird's eye view overview of Weaviate. Both human to machine and M2M communication is done over the RESTful endpoint. The ontology is validated through the semantic interface which translates all humanly readable requests into vector-based requests. 

### Database Connector & Graph Interface

Weaviate can support any database of your choosing. In the table below there is an overview of Weaviate functions that need to be implemented per database. In the folder [`/connectors`](https://github.com/creativesoftwarefdn/weaviate/tree/master/connectors) you can find all available connectors. There is a template which you can use to create a new connector called [foobar](https://github.com/creativesoftwarefdn/weaviate/blob/develop/connectors/foobar/connector.go) (documentation included inline). You can also [take inspiration](https://github.com/creativesoftwarefdn/weaviate/blob/develop/connectors/cassandra/connector.go) from the full-fledged Cassandra connector. In case of questions, please use [Stackoverflow](https://stackoverflow.com/questions/tagged/weaviate) or [Github](https://github.com/creativesoftwarefdn/weaviate/issues).

> Currently Weaviate only support Cassandra. Feel free to add or [request](https://github.com/creativesoftwarefdn/weaviate/issues/new) new connectors.

| Function           | Description                                                  | Select  | Where 1                                  | Where 2                          | Limit      | Order     | Delete | Solution                 |
| ------------------ | ------------------------------------------------------------ |:-------:| ---------------------------------------- | -------------------------------  | ---------  | --------  |:------:| ------------------------ | 
| `Init()`           | Initialize database needs the current root key               | `count` | `type = 'Key'`                           | `properties.root = 'true'`       | 1          |           | 0      | - Table 1, Index on type |
| `GetThing()`       | Get a single thing based on uuid                             | `*`     | `uuid = uuid`                            |                                  | 1          | timestamp | 1      |                          |
| `GetThings()`      | Get batch of things based on uuid                            | `*`     | `uuid IN [uuid]`                         |                                  | 1 per uuid | timestamp | 1      |                          |
| `ListThings()`     | Get list of things based on key-uuid, sorted by most recent  | `*`     | `owner = uuid`                           | _multiple where’s for searching_ | X          | timestamp | 1      | - Filter after loading   |
| `GetAction()`      | Get single action based on uuid                              | `*`     | `uuid = uuid`                            |                                  | 1          | timestamp | 1      |                          |
| `GetActions()`     | Get batch of actions based on uuid                           | `*`     | `uuid IN [uuid]`                         |                                  |            |           | 1      |                          |
| `ListAction()`     | Get list of actions based on key-uuid, sorted by most recent | `*`     | `properties.things.object.cref = 'uuid'` | _multiple where’s for searching_ | X          | timestamp | 1      | - Filter after loading   |
| `ValidateToken()`  | Validate token based on given property 'token'               | `*`     | `type = 'Key'`                           | `properties.token = 'uuid'`      | 1          |           | 1      |                          |
| `GetKey()`         | Get a single key based on uuid                               | `*`     | `uuid = uuid`                            |                                  | 1          |           | 1      |                          |
| `GetKeyChildren()` | Get batch of keys based on parent key in properties          | `*`     | `properties.parent.cref = 'uuid'`        |                                  |            | timestamp | 0      |                          |

### Ontology

Every Weaviate instance needs to have two ontologies, one for Things and one for Actions. Ontologies are always; class-, property-, value-based and classes and properties are enriched by a keyword.

| Name             | Type     | Should be in Vector? | Mandatory? | Description |
| ---------------- |:--------:|:--------------------:|:----------:|-------------|
| Class            | `string` | `true`               | `true`     | Noun for Things (i.e., "Place"), verb for action (i.e., "Bought" or "Buys") |
| Class keyword    | `array`  | `true`               | `false`    | An array of descriptions relative to the class. (i.e., the class "Place" might gave: "City" as a keyword) |
| Property         | `string` | `true`               | `true`     | Property of the class. (i.e., "name" for "City") |
| Property keyword | `array`  | `true`               | `false`    | An array of descriptions relative to the class. (i.e., the class "Place" might gave: "City" as a keyword) |
| Value            | `string` | `false`              | `true`     | Value or refererence. |

#### Keyword Characteristics in the Ontology

Keywords are used to determine the context of the word, based on the location of the word in the vector space.

Keywords are always stored in an array containing a weight. The keyword itself should be available in the vector space. The weights are -based on the chosen algorithm- used to determine the location of the class or property that the word depicts.

The following excerpt depicts how the location in the vector space is determined to represent a "place that people live in."

```
...
"class": "Place",
"description": "This is a place that people live in",
"keywords": [
  {
    "keyword": "city",
    "weight": 0.9
  },
  {
    "keyword": "town",
    "weight": 0.8
  },
  {
    "keyword": "village",
    "weight": 0.7
  },
  {
    "keyword": "people",
    "weight": 0.2
  }
],
...
```

_Note:_<br>
Both [CamelCase](https://en.wikipedia.org/wiki/Camel_case) and camelCase are interpreted as being two words. Snake_case will be interpreted as one word and most probably will not be available in the vector space.

_Note II:_<br>
Although there is no distinction being made in the vector space between uppercase and lowercase. It is advised to keep classes CamelCase (or start with capital) and properties camelCase (start with lower).

#### Value Types

List of value types as defined in the ontologies;

```json
[{
    "name": "testString",
    "@dataType": [
      "string"
    ],
    "description": "Value of testString."
  },
  {
    "name": "testInt",
    "@dataType": [
      "int"
    ],
    "description": "Value of testInt."
  },
  {
    "name": "testBoolean",
    "@dataType": [
      "boolean"
    ],
    "description": "Value of testBoolean."
  },
  {
    "name": "testNumber",
    "@dataType": [
      "number"
    ],
    "description": "Value of testNumber."
  },
  {
    "name": "testDateTime",
    "@dataType": [
      "date"
    ],
    "description": "Value of testDateTime."
  },
  {
    "name": "testCref",
    "@dataType": [
      "TestThing2"
    ],
    "description": "Value of testCref."
  }
]
```

#### Example

_Also see [this](https://github.com/creativesoftwarefdn/weaviate-semantic-schemas) repo for more examples._

```json
{
  "@context": "http://example.org",
  "classes": [
    {
      "class": "City",
      "description": "This is a test City",
      "keywords": [
        {
          "keyword": "Place",
          "weight": 1
        }
      ],
      "properties": [
        {
          "@dataType": [
            "string"
          ],
          "description": "name of the city.",
          "keywords": [
            {
              "keyword": "keyword",
              "weight": 1
            }
          ],
          "name": "name"
        },
        {
          "@dataType": [
            "int"
          ],
          "description": "Year of establishment.",
          "keywords": [
            {
              "keyword": "keyword",
              "weight": 1
            }
          ],
          "name": "established"
        },
        {
          "@dataType": [
            "number"
          ],
          "description": "Number of inhabitants.",
          "keywords": [
            {
              "keyword": "keyword",
              "weight": 1
            }
          ],
          "name": "inhabitants"
        },
        {
          "@dataType": [
            "Country"
          ],
          "description": "Country that the city is located in.",
          "keywords": [
            {
              "keyword": "keyword",
              "weight": 1
            }
          ],
          "name": "country"
        }
      ]
    },
    {
      "class": "Country",
      "description": "This is a Country",
      "keywords": [
        {
          "keyword": "Place",
          "weight": 1
        }
      ],
      "properties": [
        {
          "@dataType": [
            "string"
          ],
          "description": "Name of the country.",
          "keywords": [
            {
              "keyword": "keyword",
              "weight": 1
            }
          ],
          "name": "name"
        }
      ]
    }
  ],
  "maintainer": "hello@creativesoftwarefdn.org",
  "name": "example.org - Thing Test",
  "type": "thing",
  "version": "1.0.0"
}
```

### P2P Network

Weaviate can run as a stand-alone service or as a node on a peer to peer (P2P) network.

The P2P network is a [Hybrid P2P system](https://en.wikipedia.org/wiki/Peer-to-peer#Hybrid_models), that consists of a small [Genesis Server](./genesis/)
and the Weaviate peers. The Genesis Server is the only used to bootstrap and maintain the Peer to Peer network, the peers communicate directly with each other.

The system uses the following simple protocols to ensure that peers get registered, and that all peers are informed about all the other peers in the network.

** Registration protocol**
1. The Genesis service is running, and has no registered nodes.
2. A weaviate configured to connect to the network run by the Genesis service starts up. It registers itself in the Genesis service. (via `/peers/register`).
3. The genesis server checks that it can connect to the peer (via `/p2p/ping ), if so, it will update it's list of peers and notifies all known peers that an updated list of peers is available.
3. The new weaviate service will then receive information about which contextionary is used by the network. This is the response to the same `/peers/register` request it performed to register itself.  For now, the weaviate instance will abort if this is another contextionary than that is configured in the local Weaviate.

At the same time, we need to keep making sure that all the peers are up and running, this happens via the liveness protocol:

** Liveness protocol **
1. Each registered weaviate peer will ping the Genesis server every once in a while to make sure that it is not considered to be dead. (via `/peers/$peer_id/ping`)
2. The Genesis server will check  when the last communcation occured with each peer. If this is too long ago for some peer, it will remove that peer from the list of known peers, and issue another update to all remaining peers.


We also support gracefull deregistrations:

** Deregistration protocol **
1. If the Weaviate server is being stopped, it will deregister itself with the Genesis server via a DELETE on `/peers/$peer_id`.
2. The Geneses server updates its list of peers and issues an update to remaining peers.


| Name               | Definition |
|--------------------|------------|
| New Weaviate       | A new node on the network, if this Weaviate becomes part of the network, it becomes a Bootstrap Weaviate|
| Bootstrap Weaviate | A functional node on the network == a Bootstrap Weaviate. The bootstrap node contains enough information to onboard another node |
| Genesis Weaviate   | Is the first node on the network, after a second node is added the Genesis Weaviate becomes a bootstrap weaviate |

#### Defining a Genesis Weaviate

In the configuration add the following object:

```

{
    "environments": [{
        ...
        "P2P: {
            "genesis": true
        }
        ...
    }]
}
```

#### Defining a Weaviate as node

In the configuration add the following object:

```

{
    "environments": [{
        ...
        "P2P: {
            "bootstrappedPeers": ["URL"],
            "requestContextionary": boolean
        }
        ...
    }]
}
```

#### Initiating the P2P network

The following steps are part of starting up a Weaviate.

##### Genesis Weaviate

1. If a node is started, validate if `.environments[x].P2P` object is available. If false, run as stand-alone Weaviate and disable `/P2P/*` endpoints. If true;
2. Validate if `"genesis": true`, if yes, listen to `/P2P/*` endpoints. If succesful;
3. This Weaviate now became a Bootstrap Weaviate.

![Weaviate P2P Image 1](https://raw.githubusercontent.com/creativesoftwarefdn/weaviate/develop/assets/img/p2p-img1.jpg "Weaviate P2P Image 1")

##### New Weaviate

1. If `"bootstrappedPeers": []` is set, make the New Weaviate known to _one of the peers_ in the array on the P2P endpoint (`"operationId": "weaviate.peers.announce"`). The peer responds with the contextionaryMD5 (as string) in the body.
2. The New Weaviate validates the MD5 of the network-contextionary. If false and `requestContextionary == true` the contextionary is requested from the node in the network otherwise the Weaviate startup should fail.
3. If successful (HTTP 200 received) the New Weaviate became a Bootstrapped Weaviate.

![Weaviate P2P Image 2](https://raw.githubusercontent.com/creativesoftwarefdn/weaviate/develop/assets/img/p2p-img2.jpg "Weaviate P2P Image 2")

_The following steps are part of running a Weaviate as a node in the network._

##### Bootstrapped Weaviate

1. If a running Bootstrapped Weaviate receives a request on the `/P2P/announce` (`"operationId": "weaviate.peers.announce"`) end-point;
2. the existence of the New Weaviate is validated by requesting a `/P2P/echo` (`"operationId": "weaviate.peers.echo"`) from the New Weaviate to validate it is available.
3. In case of a 200 response, validate if the New Weaviate already is known. If true, do nothing. If false;
4. Store the meta-information of the New Weaviate and broadcasts to _all_ known nodes the existence of the New Weaviate via the `/P2P/announce` endpoint (`"operationId": "weaviate.peers.announce"`).

![Weaviate P2P Image 3](https://raw.githubusercontent.com/creativesoftwarefdn/weaviate/develop/assets/img/p2p-img3.jpg "Weaviate P2P Image 3")

#### Question the Network

1. An end-user defines a question through the `Network` search in the GraphQL endpoint. 
2. The question is translated into a network question and broadcasted to all peers via the `/peers/questions` endpoint (`"operationId": "weaviate.peers.questions.create"`).
3. A peer responds with status code 200 and an answer-UUID.
4. The node waits* for a response on the `/peers/answers/{answerId}` endpoint (`"operationId": "weaviate.peers.answers.create"`).
5. All answers are combined and sent to the end-user.

![Weaviate P2P Image 4](https://raw.githubusercontent.com/creativesoftwarefdn/weaviate/develop/assets/img/p2p-img4.jpg "Weaviate P2P Image 4")

_*- The end-user defines a "networkTimeout". This is the time it might take for the answer to accumulate._

### Network Questionnaires

_TBD_

## Usage

Weaviate is [open source](LICENSE.md); information for commercial use can be found on [www.semi.network](https://www.semi.network).

## RESTful API

[Full Open API docs]().

<!-- markdown-swagger -->
 Endpoint                      | Method | Auth? | Description                                                                                                                                                            
 ----------------------------- | ------ | ----- | -----------------------------------------------------------------------------------------------------------------------------------------------------------------------
 `/actions`                    | POST   | No    | Registers a new action. Given meta-data and schema values are validated.                                                                                               
 `/actions/validate`           | POST   | No    | Validate an action's schema and meta-data. It has to be based on a schema, which is related to the given action to be accepted by this validation.                     
 `/actions/{actionId}`         | DELETE | No    | Deletes an action from the system.                                                                                                                                     
 `/actions/{actionId}`         | GET    | No    | Lists actions.                                                                                                                                                         
 `/actions/{actionId}`         | PATCH  | No    | Updates an action. This method supports patch semantics. Given meta-data and schema values are validated. LastUpdateTime is set to the time this function is called.   
 `/actions/{actionId}`         | PUT    | No    | Updates an action's data. Given meta-data and schema values are validated. LastUpdateTime is set to the time this function is called.                                  
 `/actions/{actionId}/history` | GET    | No    | Returns a particular action history.                                                                                                                                   
 `/graphql`                    | POST   | No    | Get an object based on GraphQL                                                                                                                                         
 `/keys`                       | POST   | No    | Creates a new key. Input expiration date is validated on being in the future and not longer than parent expiration date.                                               
 `/keys/me`                    | GET    | No    | Get the key-information of the key used.                                                                                                                               
 `/keys/me/children`           | GET    | No    | Get children of used key, only one step deep. A child can have children of its own.                                                                                    
 `/keys/{keyId}`               | DELETE | No    | Deletes a key. Only parent or self is allowed to delete key. When you delete a key, all its children will be deleted as well.                                          
 `/keys/{keyId}`               | GET    | No    | Get a key.                                                                                                                                                             
 `/keys/{keyId}/children`      | GET    | No    | Get children of a key, only one step deep. A child can have children of its own.                                                                                       
 `/keys/{keyId}/renew-token`   | PUT    | No    | Renews the related key. Validates being lower in tree than given key. Can not renew itself, unless being parent.                                                       
 `/meta`                       | GET    | No    | Gives meta information about the server and can be used to provide information to another Weaviate instance that wants to interact with the current instance.          
 `/peers`                      | POST   | No    | Announce a new peer, authentication not needed (all peers are allowed to try and connect). This endpoint will only be used in M2M communications.                      
 `/peers/answers/{answerId}`   | POST   | No    | Receive an answer based on a question from a peer in the network.                                                                                                      
 `/peers/echo`                 | GET    | No    | Check if a peer is alive.                                                                                                                                              
 `/peers/questions`            | POST   | No    | Receive a question from a peer in the network.                                                                                                                         
 `/things`                     | GET    | No    | Lists all things in reverse order of creation, owned by the user that belongs to the used token.                                                                       
 `/things`                     | POST   | No    | Registers a new thing. Given meta-data and schema values are validated.                                                                                                
 `/things/validate`            | POST   | No    | Validate a thing's schema and meta-data. It has to be based on a schema, which is related to the given Thing to be accepted by this validation.                        
 `/things/{thingId}`           | DELETE | No    | Deletes a thing from the system. All actions pointing to this thing, where the thing is the object of the action, are also being deleted.                              
 `/things/{thingId}`           | GET    | No    | Returns a particular thing data.                                                                                                                                       
 `/things/{thingId}`           | PATCH  | No    | Updates a thing data. This method supports patch semantics. Given meta-data and schema values are validated. LastUpdateTime is set to the time this function is called.
 `/things/{thingId}`           | PUT    | No    | Updates a thing data. Given meta-data and schema values are validated. LastUpdateTime is set to the time this function is called.                                      
 `/things/{thingId}/actions`   | GET    | No    | Lists all actions in reverse order of creation, related to the thing that belongs to the used thingId.                                                                 
 `/things/{thingId}/history`   | GET    | No    | Returns a particular thing history.                                                                                                                                    
<!-- /markdown-swagger -->

<sup>Mardown generated with `markdown-swagger OpenAPI-Specification/schema.json README.md`</sup>
