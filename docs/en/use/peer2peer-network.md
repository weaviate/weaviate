# Peer to Peer (P2P) Network

> SHORT DESCRIPTION OF THIS DOC

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

A local Weaviate's GraphQL endpoint can be used to query the network.

```graphql
{
    Network{
        # Query goes here
    }
}
```

For more information also see [this article about GraphQL Network queries](./graphql_network.md).



































































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