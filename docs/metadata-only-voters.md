# Metadata-only voters (separating quorum from data)

Keep a fixed, small RAFT quorum while scaling data nodes independently. With
`RAFT_METADATA_ONLY_VOTERS` enabled, the RAFT voters hold the schema and form
the quorum but store **no shard data**; the non-voter nodes hold all the data.
Adding or removing data nodes never changes the quorum.

Voters still run the coordinator layer, so a read or write that lands on a voter
is transparently proxied to the data nodes that own the shards. A single load
balancer can therefore target every node.

This is **opt-in**: with `RAFT_METADATA_ONLY_VOTERS` unset, behavior is unchanged.

## Roles

A node is a **voter** if its name is among the first `RAFT_BOOTSTRAP_EXPECT`
entries of `RAFT_JOIN`. Every other node joins as a **non-voter** (a follower
that replicates the RAFT log but can never be elected leader). With the flag on:

| | RAFT role | Holds shards? | Coordinates requests? |
|---|---|---|---|
| Voters (first `BOOTSTRAP_EXPECT` of `RAFT_JOIN`) | voter, quorum | no | yes |
| All other nodes | non-voter | yes | yes |

## Enabling it

Set the **same** env on every node; the role is derived from the hostname:

```
RAFT_METADATA_ONLY_VOTERS=true
RAFT_BOOTSTRAP_EXPECT=3
RAFT_JOIN=voter-0,voter-1,voter-2        # voters only — never list data nodes here
CLUSTER_JOIN=voter-0:7100,voter-1:7100,voter-2:7100   # gossip seed, reachable by all
```

`RAFT_JOIN` lists only the voters. Data nodes are discovered through the
memberlist gossip layer (`CLUSTER_JOIN`) and join as non-voters; they are not
named in `RAFT_JOIN`. In Kubernetes this maps cleanly to two StatefulSets (a
voter set and a data set) with one shared env template, behind one Service.

## Footguns

- **Set the env var on every node, not just the voters.** Each node computes the
  voter (non-storage) set locally to decide shard placement. A node that does not
  have the flag set could place shards on a voter, which holds no data.
- **Provision enough data nodes for your replication factor.** Voters are never
  placement candidates, so a collection's replication factor must be `<=` the
  number of data nodes (not the total node count). Otherwise collection creation
  fails with `could not find enough weaviate nodes for replication`.
- **Fresh clusters only.** Enabling this does not migrate shards off nodes that
  already hold data. Start with the topology in place.
- **Voters are not free.** They run the coordinator/index layer (in-memory index
  per collection, no shards), so they use more memory than a pure quorum process,
  though far less than a data node. Size them with collection count in mind.
- **Quorum is the voter count.** With `RAFT_BOOTSTRAP_EXPECT=3` you tolerate one
  voter failure; losing two voters stalls the cluster regardless of data-node
  count.
