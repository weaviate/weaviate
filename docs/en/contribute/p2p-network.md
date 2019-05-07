# P2P Network

> The p2p network enables `{ Network { ... } }` queries and connects different
> peers. From a developers perspective it's mostly just a simple proxying
> mechanism.

## Pluggable Networks

Networks are designed to be pluggable, however they're not fully pluggable at
the moment. There is an [issue to resolve
this](https://github.com/semi-technologies/weaviate/issues/621). However,
this only becomes urgent once we decide we need a different network style.

As of now, whenever we talk about network, we mean the [Hybrid P2P Network over
HTTP(S)](../use/peer2peer-network.md) which is already extensively documented
in the user-facing docs.

Additionally in here we want to highlight how a couple of (mostly
architectural) concepts work. All following sections assume the hybrid p2p
model. They might also hold true for another system in the future, but since
there is no other system yet, we don't know yet.

## Keeping everything in sync

As outlined in the user-docs, the Genesis makes sure that every local instance
always contains a list of all peers with the current schema hash.

When consuming the updated peer list in the `network` package, we also download
the updated schema straight from the peer if an update is required.

This means the list of peers (that is also used in the `graphqlapi` package
always contains up2date info about the peers connection info, schema hash and a
copy of the current schema. The graphQL endpoints in turn are designed to be
rebuilt whenever:
- either the local db schema changes
- any of the network peers change

## Resolve Network Queries

When resolving network queries we try to reuse as much as we can from Local
queries where it makes sense. For an example of how we cando this see the first
query, `Network.Get`.

### Resolve `Network.Get.<peerName>.<kind>.<className>`

Resolving a `Network.Get` query has one advantage: The user already specified
previosly from which network peer they want to querry info. This means that we
can in turn translate this query into a Local query for that particular
instance. So, whenever an instance receives a `Network.Get` query, it merely
acts as a proxy to the remote peer.

As an example look at the following query.

1. Instance `WeaviateA` receives the following query: `{ Network { Get {
   WeaviateB { Things { City { name } } } } } }`
1. It extracts to pieces of Info from that query: The user wants info from
   remote peer `WeaviateB`. From B's schema the user wants to have all `Things`
   with class `City` and only the `name` properties
1. This is enough info to form a local query against that particular peer.
   Weaviate now builds the following query: `{ Local { Get { Things { City {
   name } } } } }` and sends it to the peer `WeaviateB`. All auth that the user
   specified simply get proxied as well.
1. Peer `WeaviateB` resopnds normally, it doesn't even know that it just
   responded to a Network query.
1. `WeaviateA` extracts the info from the `Local.Get` query and places it at
   the right point into the Network query and responds to the user.

### Resolve `Network.GetMeta.<peerName>.<kind>.<className>`

Not implemented yet, see [issue #644](https://github.com/semi-technologies/weaviate/issues/644).

### Resolve `Network.Fetch.<kind>`

Not implemented yet, see [issue #645](https://github.com/semi-technologies/weaviate/issues/645).

### Resolve `Network.Fetch.Fuzzy`

Not implemented yet, see [issue #647](https://github.com/semi-technologies/weaviate/issues/647).

### Resolve `Network.Fetch.Fuzzy`

Not implemented yet, see [issue #648](https://github.com/semi-technologies/weaviate/issues/648).
