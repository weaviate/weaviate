//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package shard

// PreferredBirthLeader selects the voter that should campaign first when a
// shard's raft group is born, so first-election leadership spreads evenly
// across the replica nodes instead of following the randomized election-timer
// race. It is a pure function of metadata every voter already shares through
// the schema (the replicated member order and the class's tenant count), so
// all voters of a group agree on the designation with no coordination and no
// wire exchange — each node only ever compares the result against its own ID.
//
// members must be the shard's replica set in replicated schema order
// (Physical.BelongsToNodes); passing it in any other order breaks cross-node
// agreement. Returns "" for an empty member list.
//
// Single-tenant (multiTenant=false): returns members[0]. The sharding-state
// generator assigns each successive shard of a class a head node one step
// further around the node ring (see sharding.State.initPhysical), so the
// heads of a class's shards are an exact round-robin over the nodes — this is
// the stable, schema-persisted preferred leader (the first replica in the
// list, as in Kafka), and the anchor a future rebalancer can reconcile
// leadership back toward at any time.
//
// Multi-tenant (multiTenant=true): returns members[tenantCount % len], where
// tenantCount is the class's physical shard (tenant) count as read at group
// birth. Within one tenant batch the count is constant while the generator
// rotates the heads, and across separate creations the count strides while
// the heads stay put — both compose to round-robin leader placement in
// creation order. This branch is birth-time-only semantics: the count moves
// with tenant churn and COLD→HOT activation timing, so re-evaluating it later
// yields a different node than at birth. It must never be treated as a stable
// placement contract — post-birth drift is owned by dynamic (count-balancing)
// rebalancing, not by this function.
//
// The designation is a hint, not an invariant: if the designated node is down,
// slow, or disagrees (activation-timing windows), the group falls back to the
// normal randomized election unchanged.
func PreferredBirthLeader(members []string, multiTenant bool, tenantCount int) string {
	if len(members) == 0 {
		return ""
	}
	if !multiTenant {
		return members[0]
	}
	if tenantCount < 0 {
		tenantCount = 0
	}
	return members[tenantCount%len(members)]
}
