> We are updating our docs and they will be moved to [www.semi.technology](https://www.semi.technology) soon.
> You can leave your email [here](http://eepurl.com/gye_bX) to get a notification when they are live.

# Peer to Peer (P2P) Network

> An overview of Weaviate's peer to peer network and how to access it.

Weaviate can run as a stand-alone service or as a node on a peer-to-peer (P2P)
network. Networks are designed in a pluggable fashion, however as of now the
only available network is a Hybrid P2P network over HTTP.

## Hybrid P2P Network over HTTP(S)

The P2P network is a [Hybrid P2P
system](https://en.wikipedia.org/wiki/Peer-to-peer#Hybrid_models), that
consists of a small [Genesis Server](./genesis/) and the Weaviate peers. The
Genesis Server is the only used to bootstrap and maintain the Peer to Peer
network, the peers communicate directly with each other when data from a remote
peer is queried.

### How it works - components of the p2p network

The system uses the following simple protocols to ensure that peers get
registered, and that all peers are informed about all the other peers in the
network.

#### Registration protocol

1. The Genesis service is running, and has no registered nodes.
2. A Weaviate configured to connect to the network run by the Genesis service
starts up. It registers itself in the Genesis service. (via `/peers/register`).
3. The genesis server checks that it can connect to the peer (via `/p2p/ping ).
If so, it will update its list of peers and notifies all known peers that an
updated list of peers is available.
3. The new Weaviate service will then receive information about which
Contextionary is used by the network. This is the response to the same
`/peers/register` request it performed to register itself.  For now, the
weaviate instance will abort if this is another Contextionary than that which
is configured in the local Weaviate.

At the same time, we need to keep making sure that all the peers are up and
running, this happens via the _liveness_ protocol:

#### Liveness protocol
1. Each registered Weaviate peer will ping the Genesis Server every once in a
while to make sure that the peer is known to be alive. (via
`/peers/$peer_id/ping`)
2. The Genesis Server will check  when the last communication occurred with
each peer. If this is too long ago for a peer, it will remove that peer from
the list of known peers, and issue another update to all remaining peers.

Additionally during each liveness ping, an instance will include a hash of its
current schema. This way peers can find out whether they need to initiate a new
download of the remote peer's schema or the currently cached version is still
accurate.

We also support graceful deregistrations:

#### Deregistration protocol
1. If the Weaviate server is being stopped, it will deregister itself with the
Genesis Server via a DELETE on `/peers/$peer_id`.
2. The Genesis Server updates its list of peers and issues an update to the
remaining peers.

### How to use 

The p2p setup is designed be easy to use and does not require much other than
http(s) access between the peers and the genesis.

#### Deploying a Genesis

Before any peer can joing the p2p network, the genesis server must be deployed.
The genesis-server is a stand-alone application and is not tied to a particular
weaviate instance.

Make sure that 

1. the Genesis can reach all future peers. This is necessary, because the
   genesis will send broadcast updates, for example when new peers join. (*This
   has less to do with the genesis setup and more with the setup of the peers*)
1. all (future) peers can reach the Genesis. This is necessary, because every
   peer has to regularly send a keep-alive ping including the current schema
   hash to the genesis server. 

The easiest is if both the genesis and all peers are
- either publicly exposed on the internet
- or in the same network
- or in connected networks (VPC tunneling, etc.)

#### Register your first peer

Once the genesis is up and running any weaviate instance can register with this
particular genesis to join the network. No manual steps are required. Weaviate
will do the registration itself on startup if it is configured correctly.

To register your peer specify the following configuraiton in your
`weaviate.conf.json`:

```json
{
  "environments":[{
    ...
    "network": {
      "genesis_url": "https://genesis.example.com",
      "public_url": "https://weaviateA.example.com",
      "peer_name": "WeaviateA"
    }
  }]
}
```

In the example above

- `genesis_url` is the URL to the previously deployed genesis. This URL must be
  reachable from where the local peer is located.
- `public_url` is how the Genesis and all other peers in the network can reach
  this peer. This in turn needs to be reachable from the location of the
  Genesis. 
- `peer_name` is the name to be used in the API to reference this particular
  peer. For example a seconde peer might want to query data from the peer
  currently being registered. It would form a query like `{ Network { Get {
  WeaviateA { Things { City } } } } }`.

**Important: Make sure that all peers and the genesis in the network can reach your local
peer under the specific `public_url`!**

#### Ask your Network a question

A local Weaviate's GraphQL endpoint can be used to query the network.

```graphql { Network{ # Query goes here } } ```

For more information also see [this article about GraphQL Network
queries](./graphql_network.md).

