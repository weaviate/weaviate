package types

import (
	"math/rand"
	"sync"
)

// IntFunc is a light-weight alias used for dependency-injecting a source
// of pseudo-random numbers (typically math/rand.Intn).  Any function that
// matches the signature `func(int) int` is accepted.
type IntFunc func(int) int

// ReplicaPicker selects *exactly one* replica per shard from the provided
// ReplicaSet.  Concrete strategies can differ (random, round-robin, latency
// aware, …) but must never return two replicas that share the same shard (to
// avoid reading duplicate data).
type ReplicaPicker interface {
	Pick(in ReplicaSet) ReplicaSet
}

// RandomReplicaPicker picks a single replica per shard using the supplied
// RNG. The goal behind random replica selection is to spread the read load among multiple
// read replicas avoiding some nodes becoming a hot-spot. Some random number generators are
// not suitable to be shared among multiple goroutines as they normally keep some state that
// needs to be protected for concurrent access. For this reason we protect usage of the random
// number generator with a mutex.
type RandomReplicaPicker struct {
	mu  sync.Mutex
	rng IntFunc
}

func NewRandomReplicaPicker(rng IntFunc) ReplicaPicker {
	if rng == nil {
		rng = rand.Intn
	}
	return &RandomReplicaPicker{rng: rng}
}

func (p *RandomReplicaPicker) rand(n int) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.rng(n)
}

func (p *RandomReplicaPicker) Pick(in ReplicaSet) ReplicaSet {
	replicas := in.Replicas
	if len(replicas) < 2 {
		return in
	}

	buckets := make(map[string][]int, len(replicas))
	for i, replica := range replicas {
		buckets[replica.ShardName] = append(buckets[replica.ShardName], i)
	}

	out := make([]Replica, 0, len(buckets))
	for _, bucket := range buckets {
		randomReplica := p.rand(len(bucket))
		out = append(out, replicas[bucket[randomReplica]])
	}
	return ReplicaSet{Replicas: out}
}

// DirectCandidateReplicaPicker prefers the replica whose NodeName matches
// the direct candidate.  If the preferred node is not present for a shard, the
// decision is delegated to the provided fallbackPicker (random by default).
// Selecting a node that matches the direct candidate node favor local reads
// versus remote reads reducing network traffic and ideally improving read latency.
type DirectCandidateReplicaPicker struct {
	directCandidate string
	fallbackPicker  ReplicaPicker
}

// NewDirectCandidateReplicaPicker builds a picker that prioritises
// directCandidate.  When fallbackPicker is nil, a RandomReplicaPicker that
// uses math/rand.Intn is created automatically.
func NewDirectCandidateReplicaPicker(directCandidate string, fallbackPicker ReplicaPicker) ReplicaPicker {
	if fallbackPicker == nil {
		fallbackPicker = NewRandomReplicaPicker(rand.Intn)
	}
	return &DirectCandidateReplicaPicker{
		directCandidate: directCandidate,
		fallbackPicker:  fallbackPicker,
	}
}

// Pick chooses one replica per shard, favouring the direct candidate when present
// and otherwise deferring to the fallback picker.
func (p *DirectCandidateReplicaPicker) Pick(in ReplicaSet) ReplicaSet {
	replicas := in.Replicas
	if len(replicas) < 2 || p.directCandidate == "" {
		return p.fallbackPicker.Pick(in)
	}

	buckets := groupByShard(replicas)
	out := make([]Replica, 0, len(buckets))

	for _, ids := range buckets {
		// try to find a replica on a node that matches the direct candidate node
		directCandidateFound := false
		for _, i := range ids {
			if replicas[i].NodeName == p.directCandidate {
				out = append(out, replicas[i])
				directCandidateFound = true
				break
			}
		}
		if directCandidateFound {
			continue // move to next shard
		}

		// no direct candidate, use a random replica from a set of replicas for a shard
		bucket := ReplicaSet{Replicas: make([]Replica, 0, len(ids))}
		for _, i := range ids {
			bucket.Replicas = append(bucket.Replicas, replicas[i])
		}
		out = append(out, p.fallbackPicker.Pick(bucket).Replicas[0])
	}

	return ReplicaSet{Replicas: out}
}

// groupByShard builds a map shardName to indices in the original slice.
func groupByShard(replicas []Replica) map[string][]int {
	m := make(map[string][]int, len(replicas))
	for i, replica := range replicas {
		m[replica.ShardName] = append(m[replica.ShardName], i)
	}
	return m
}
