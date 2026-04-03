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

package replica

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/storobj"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
)

var (
	// MsgCLevel consistency level cannot be achieved
	MsgCLevel = "cannot achieve consistency level"

	ErrReplicas = errors.New("cannot reach enough replicas")
	ErrRepair   = errors.New("read repair error")
	ErrRead     = errors.New("read error")

	ErrNoDiffFound = errors.New("no diff found")

	// ErrHashtreeRootUnchanged is returned by CollectShardDifferences when the
	// remote node's hashtree root has not changed since the last propagation
	// cycle, meaning the remote has not yet flushed the objects that were sent
	// to it. The caller should keep the fast-poll cadence active and skip the
	// full CompareDigests + propagation pass until the remote root changes.
	ErrHashtreeRootUnchanged = errors.New("hashtree root unchanged since last propagation cycle")
)

type (
	// senderReply is a container for the data received from a replica
	senderReply[T any] struct {
		sender     string // hostname of the sender
		Version    int64  // sender's current version of the object
		Data       T      // the data sent by the sender
		UpdateTime int64  // sender's current update time
		DigestRead bool
	}
	findOneReply senderReply[Replica]
	existReply   struct {
		Sender string
		types.RepairResponse
	}
)

// Finder finds replicated objects
type Finder struct {
	router       types.Router
	nodeResolver cluster.NodeResolver
	nodeName     string
	finderStream // stream of objects
}

// NewFinder constructs a new finder instance
func NewFinder(className string,
	router types.Router,
	nodeResolver cluster.NodeResolver,
	nodeName string,
	client RClient,
	metrics *Metrics,
	l logrus.FieldLogger,
	getDeletionStrategy func() string,
) *Finder {
	cl := FinderClient{client}
	return &Finder{
		router:       router,
		nodeResolver: nodeResolver,
		nodeName:     nodeName,
		finderStream: finderStream{
			repairer: repairer{
				class:               className,
				getDeletionStrategy: getDeletionStrategy,
				client:              cl,
				metrics:             metrics,
				logger:              l,
			},
			log: l,
		},
	}
}

// GetOne gets object which satisfies the giving consistency
func (f *Finder) GetOne(ctx context.Context,
	l types.ConsistencyLevel, shard string,
	id strfmt.UUID,
	props search.SelectProperties,
	adds additional.Properties,
) (*storobj.Object, error) {
	c := NewReadCoordinator[findOneReply](f.router, f.metrics, f.class, shard, f.getDeletionStrategy(), f.log)
	op := func(ctx context.Context, host string, fullRead bool) (findOneReply, error) {
		if fullRead {
			r, err := f.client.FullRead(ctx, host, f.class, shard, id, props, adds, 0)

			return findOneReply{host, 0, r, r.UpdateTime(), false}, err
		} else {
			xs, err := f.client.DigestReads(ctx, host, f.class, shard, []strfmt.UUID{id}, 0)

			var x types.RepairResponse

			if len(xs) == 1 {
				x = xs[0]
			}

			r := Replica{
				ID:                      id,
				Deleted:                 x.Deleted,
				LastUpdateTimeUnixMilli: x.UpdateTime,
			}

			return findOneReply{host, x.Version, r, x.UpdateTime, true}, err
		}
	}
	replyCh, level, err := c.Pull(ctx, l, op, "", 20*time.Second)
	if err != nil {
		f.log.WithField("op", "pull.one").Error(err)
		return nil, fmt.Errorf("%s %q: %w", MsgCLevel, l, ErrReplicas)
	}
	result := <-f.readOne(ctx, shard, id, replyCh, level)
	if err = result.Err; err != nil {
		err = fmt.Errorf("%s %q: %w", MsgCLevel, l, err)
		if strings.Contains(err.Error(), ErrConflictExistOrDeleted.Error()) {
			err = objects.NewErrDirtyReadOfDeletedObject(err)
		}
	}
	return result.Value, err
}

func (f *Finder) FindUUIDs(ctx context.Context, className, shard string,
	filters *filters.LocalFilter, l types.ConsistencyLevel, limit int,
) (uuids []strfmt.UUID, err error) {
	c := NewReadCoordinator[[]strfmt.UUID](f.router, f.metrics, f.class, shard, f.getDeletionStrategy(), f.log)

	op := func(ctx context.Context, host string, _ bool) ([]strfmt.UUID, error) {
		return f.client.FindUUIDs(ctx, host, f.class, shard, filters, limit)
	}

	replyCh, _, err := c.Pull(ctx, l, op, "", 30*time.Second)
	if err != nil {
		f.log.WithField("op", "pull.one").Error(err)
		return nil, fmt.Errorf("%s %q: %w", MsgCLevel, l, ErrReplicas)
	}

	res := make(map[strfmt.UUID]struct{})
	anyOk := false
	ec := errorcompounder.New()

	for r := range replyCh {
		if r.Err != nil {
			ec.Add(r.Err)
			f.logger.WithField("op", "finder.find_uuids").WithError(r.Err).Debug("error in reply channel")
			continue
		}

		anyOk = true
		for _, uuid := range r.Value {
			res[uuid] = struct{}{}
		}
	}

	if !anyOk {
		return nil, ec.ToError()
	}

	count := len(res)
	if limit > 0 {
		count = min(limit, len(res))
	}
	uuids = make([]strfmt.UUID, count)
	i := 0
	for uuid := range res {
		uuids[i] = uuid
		i++
		if i == count {
			break
		}
	}
	return uuids, nil
}

type ShardDesc struct {
	Name string
	Node string
}

// CheckConsistency for objects belonging to different physical shards.
//
// For each x in xs the fields BelongsToNode and BelongsToShard must be set non empty
func (f *Finder) CheckConsistency(ctx context.Context,
	l types.ConsistencyLevel, xs []*storobj.Object,
) error {
	if len(xs) == 0 {
		return nil
	}
	for i, x := range xs { // check shard and node name are set
		if x == nil {
			return fmt.Errorf("contains nil at object at index %d", i)
		}
		if x.BelongsToNode == "" || x.BelongsToShard == "" {
			return fmt.Errorf("missing node or shard at index %d", i)
		}
	}

	if l == types.ConsistencyLevelOne { // already consistent
		for i := range xs {
			xs[i].IsConsistent = true
		}
		return nil
	}
	// check shard consistency concurrently
	gr, ctx := enterrors.NewErrorGroupWithContextWrapper(f.logger, ctx)
	for _, part := range clusterObjectByShard(createBatch(xs)) {
		part := part
		gr.Go(func() error {
			_, err := f.checkShardConsistency(ctx, l, part)
			if err != nil {
				f.log.WithField("op", "check_shard_consistency").
					WithField("shard", part.Shard).Error(err)
			}
			return err
		}, part)
	}
	return gr.Wait()
}

// Exists checks if an object exists which satisfies the giving consistency
func (f *Finder) Exists(ctx context.Context,
	l types.ConsistencyLevel,
	shard string,
	id strfmt.UUID,
) (bool, error) {
	c := NewReadCoordinator[existReply](f.router, f.metrics, f.class, shard, f.getDeletionStrategy(), f.log)
	op := func(ctx context.Context, host string, _ bool) (existReply, error) {
		xs, err := f.client.DigestReads(ctx, host, f.class, shard, []strfmt.UUID{id}, 0)
		var x types.RepairResponse
		if len(xs) == 1 {
			x = xs[0]
		}
		return existReply{host, x}, err
	}
	replyCh, state, err := c.Pull(ctx, l, op, "", 20*time.Second)
	if err != nil {
		f.log.WithField("op", "pull.exist").Error(err)
		return false, fmt.Errorf("%s %q: %w", MsgCLevel, l, ErrReplicas)
	}
	result := <-f.readExistence(ctx, shard, id, replyCh, state)
	if err = result.Err; err != nil {
		err = fmt.Errorf("%s %q: %w", MsgCLevel, l, err)
		if strings.Contains(err.Error(), ErrConflictExistOrDeleted.Error()) {
			err = objects.NewErrDirtyReadOfDeletedObject(err)
		}
	}
	return result.Value, err
}

// NodeObject gets object from a specific node.
// it is used mainly for debugging purposes
func (f *Finder) NodeObject(ctx context.Context,
	nodeName,
	shard string,
	id strfmt.UUID,
	props search.SelectProperties, adds additional.Properties,
) (*storobj.Object, error) {
	host, ok := f.nodeResolver.NodeHostname(nodeName)
	if !ok || host == "" {
		return nil, fmt.Errorf("cannot resolve node name: %s", nodeName)
	}
	r, err := f.client.FullRead(ctx, host, f.class, shard, id, props, adds, 9)
	return r.Object, err
}

// checkShardConsistency checks consistency for a set of objects belonging to a shard
// It returns the most recent objects or and error
func (f *Finder) checkShardConsistency(ctx context.Context,
	l types.ConsistencyLevel,
	batch ShardPart,
) ([]*storobj.Object, error) {
	var (
		c         = NewReadCoordinator[BatchReply](f.router, f.metrics, f.class, batch.Shard, f.getDeletionStrategy(), f.log)
		shard     = batch.Shard
		data, ids = batch.Extract() // extract from current content
	)
	op := func(ctx context.Context, host string, fullRead bool) (BatchReply, error) {
		if fullRead { // we already have the content
			return BatchReply{Sender: host, IsDigest: false, FullData: data}, nil
		} else {
			xs, err := f.client.DigestReads(ctx, host, f.class, shard, ids, 0)
			return BatchReply{Sender: host, IsDigest: true, DigestData: xs}, err
		}
	}

	replyCh, state, err := c.Pull(ctx, l, op, batch.Node, 20*time.Second)
	if err != nil {
		return nil, fmt.Errorf("pull shard: %w", ErrReplicas)
	}
	result := <-f.readBatchPart(ctx, batch, ids, replyCh, state)
	return result.Value, result.Err
}

// ShardDiffSkipState carries the prior-cycle root snapshots and work flag for
// a single target node. When provided to CollectShardDifferences, the function
// short-circuits after the level-0 RPC if the skip conditions are met, saving
// the remaining (Height-1) RPCs needed for the full tree walk.
type ShardDiffSkipState struct {
	LocalRoot  hashtree.Digest
	RemoteRoot hashtree.Digest
	// HadWork is true when the prior cycle actually sent objects to this target.
	// Local-only deletions (remoteDeleted verdicts) must NOT set this flag
	// because they do not change the remote hashtree.
	HadWork bool
}

type ShardDifferenceReader struct {
	TargetNodeName    string
	TargetNodeAddress string
	RangeReader       hashtree.AggregatedHashTreeRangeReader
	// LocalHashtreeRoot and RemoteHashtreeRoot are the level-0 digests observed
	// at the time of comparison. They are zero when the comparison was skipped
	// before the level-0 RPC (e.g. early ErrNoDiffFound from target-node
	// overrides). The caller stores these in its per-target tracking maps so
	// that the next call to CollectShardDifferences can short-circuit after
	// the level-0 RPC when the skip conditions (Case A or Case B) are met.
	LocalHashtreeRoot  hashtree.Digest
	RemoteHashtreeRoot hashtree.Digest
}

// CollectShardDifferences collects the differences between the local node and the target nodes.
// It returns a ShardDifferenceReader that contains the differences and the target node name/address.
// If no differences are found, it returns ErrNoDiffFound.
// When ErrNoDiffFound is returned as the error, the returned *ShardDifferenceReader may exist
// and have some (but not all) of its fields set.
//
// skipStateByTarget is optional (may be nil). When provided, after the level-0
// RPC (which fetches both root digests with a single round-trip) the function
// checks skip conditions against the stored prior-cycle roots:
//   - Case A (ErrHashtreeRootUnchanged): remote root unchanged AND prior cycle sent objects →
//     remote hasn't flushed yet; return immediately to keep the fast-poll cadence.
//   - Case B (ErrNoDiffFound): both roots unchanged AND prior cycle sent nothing →
//     nothing has changed; skip the remaining levels.
//
// In both cases the full tree walk (up to Height-1 additional RPCs) is avoided.
func (f *Finder) CollectShardDifferences(ctx context.Context,
	shardName string, ht hashtree.AggregatedHashTree, diffTimeoutPerNode time.Duration,
	targetNodeOverrides []additional.AsyncReplicationTargetNodeOverride,
	skipStateByTarget map[string]ShardDiffSkipState,
) (diffReader *ShardDifferenceReader, err error) {
	options := f.router.BuildRoutingPlanOptions(shardName, shardName, types.ConsistencyLevelOne, "")
	routingPlan, err := f.router.BuildReadRoutingPlan(options)
	if err != nil {
		return nil, fmt.Errorf("%w : class %q shard %q", err, f.class, shardName)
	}

	collectDiffForTargetNode := func(targetNodeAddress, targetNodeName string) (*ShardDifferenceReader, error) {
		ctx, cancel := context.WithTimeout(ctx, diffTimeoutPerNode)
		defer cancel()

		// Assumes the remote shard's hashtree has the same height as the local one.
		// A height mismatch will be caught implicitly: the remote will return a
		// different digest count than expected, which is treated as an error below.
		diff := hashtree.NewBitset(hashtree.NodesCount(ht.Height()))

		// NOTE: the traversal is not atomic — concurrent writes to ht between level
		// reads may cause false-positive diff ranges. This is acceptable: propagation
		// is idempotent and false negatives cannot occur (a difference present at
		// traversal start will always be detected).
		digests := make([]hashtree.Digest, hashtree.LeavesCount(ht.Height()))

		diff.Set(0) // init comparison at root level

		// Compare levels 0 through Height-1, skipping the leaf level.
		// The leaf-level HashTreeLevel response can be very large (up to
		// 2^height × 16 bytes per call), while skipping it costs at most 2×
		// wider UUID ranges in the subsequent DigestObjectsInRange scans.
		// For a height-0 tree the root IS the leaf, so we must include it.
		loopBound := ht.Height()
		if loopBound == 0 {
			loopBound = 1
		}
		// localRoot and remoteRoot are captured at level 0 (the tree root) so the
		// caller can detect whether either side has flushed since the last cycle.
		var localRoot, remoteRoot hashtree.Digest
		for l := 0; l < loopBound; l++ {
			n, err := ht.Level(l, diff, digests)
			if err != nil {
				return nil, fmt.Errorf("%q: %w", targetNodeAddress, err)
			}

			levelDigests, err := f.client.HashTreeLevel(ctx, targetNodeAddress, f.class, shardName, l, diff)
			if err != nil {
				return nil, fmt.Errorf("%q: %w", targetNodeAddress, err)
			}
			if len(levelDigests) != n {
				// Remote returned fewer or more digests than the discriminant requested.
				// This indicates a height mismatch or an inconsistent remote state.
				// Return an error so the caller can try the next replica rather than
				// silently treating the shard as in-sync or panicking in LevelDiff.
				return nil, fmt.Errorf("%q: level %d: expected %d digests from remote, got %d",
					targetNodeAddress, l, n, len(levelDigests))
			}

			if l == 0 && n > 0 {
				localRoot = digests[0]
				remoteRoot = levelDigests[0]

				// Short-circuit using only the root digests — avoids the remaining
				// (Height-1) level RPCs when a skip condition is met.
				if prior, ok := skipStateByTarget[targetNodeName]; ok {
					remoteRootUnchanged := remoteRoot == prior.RemoteRoot
					localRootUnchanged := localRoot == prior.LocalRoot
					if remoteRootUnchanged && prior.HadWork {
						// Case A: objects were propagated last cycle but the remote
						// hasn't flushed its memtable yet. Signal the caller to keep
						// the fast-poll cadence without re-running the expensive scan.
						return &ShardDifferenceReader{
							TargetNodeName:     targetNodeName,
							TargetNodeAddress:  targetNodeAddress,
							LocalHashtreeRoot:  localRoot,
							RemoteHashtreeRoot: remoteRoot,
						}, ErrHashtreeRootUnchanged
					}
					if remoteRootUnchanged && localRootUnchanged && !prior.HadWork {
						// Case B: nothing was propagated last cycle and neither side
						// has changed — the next full scan would find the same empty
						// result. Return ErrNoDiffFound so the caller drops back to
						// the slow-poll cadence.
						return &ShardDifferenceReader{
							TargetNodeName:     targetNodeName,
							TargetNodeAddress:  targetNodeAddress,
							LocalHashtreeRoot:  localRoot,
							RemoteHashtreeRoot: remoteRoot,
						}, ErrNoDiffFound
					}
				}
			}

			levelDiffCount := hashtree.LevelDiff(l, diff, digests, levelDigests)
			if levelDiffCount == 0 {
				// no differences were found
				break
			}
		}

		// Expand any non-leaf bits set by LevelDiff to their leaf-level
		// descendants so that HashTreeDiffReader (which scans only leaf
		// positions) can enumerate the differing UUID ranges.
		expandDiscriminantToLeaves(diff, ht.Height())

		if diff.SetCount() == 0 {
			return &ShardDifferenceReader{
				TargetNodeName:     targetNodeName,
				TargetNodeAddress:  targetNodeAddress,
				LocalHashtreeRoot:  localRoot,
				RemoteHashtreeRoot: remoteRoot,
			}, ErrNoDiffFound
		}

		return &ShardDifferenceReader{
			TargetNodeName:     targetNodeName,
			TargetNodeAddress:  targetNodeAddress,
			RangeReader:        ht.NewRangeReader(diff),
			LocalHashtreeRoot:  localRoot,
			RemoteHashtreeRoot: remoteRoot,
		}, nil
	}

	ec := errorcompounder.New()

	// If the caller provided a list of target node overrides, filter the replicas to only include
	// the relevant overrides so that we only "push" updates to the specified nodes.
	localNodeName := f.LocalNodeName()
	targetNodesToUse := routingPlan.NodeNames()
	if len(targetNodeOverrides) > 0 {
		targetNodesToUse = make([]string, 0, len(targetNodeOverrides))
		for _, override := range targetNodeOverrides {
			if override.SourceNode == localNodeName && override.CollectionID == f.class && override.ShardID == shardName {
				targetNodesToUse = append(targetNodesToUse, override.TargetNode)
			}
		}
	}

	replicaNodeNames := make([]string, 0, len(routingPlan.Replicas()))
	replicasHostAddrs := make([]string, 0, len(routingPlan.HostAddresses()))
	for _, replica := range targetNodesToUse {
		replicaHostAddr, ok := f.nodeResolver.NodeHostname(replica)
		if ok {
			replicaNodeNames = append(replicaNodeNames, replica)
			replicasHostAddrs = append(replicasHostAddrs, replicaHostAddr)
		}
	}

	// shuffle the replicas to randomize the order in which we look for differences
	if len(replicasHostAddrs) > 1 {
		// Use the global rand package which is thread-safe
		rand.Shuffle(len(replicasHostAddrs), func(i, j int) {
			replicaNodeNames[i], replicaNodeNames[j] = replicaNodeNames[j], replicaNodeNames[i]
			replicasHostAddrs[i], replicasHostAddrs[j] = replicasHostAddrs[j], replicasHostAddrs[i]
		})
	}

	localHostAddr, _ := f.nodeResolver.NodeHostname(localNodeName)

	// hadUnchanged is set when at least one target returned ErrHashtreeRootUnchanged
	// (Case A). It is used only if no other target produced a real diff, so the
	// caller can preserve its fast-poll cadence.
	var hadUnchanged bool

	for i, targetNodeAddress := range replicasHostAddrs {
		targetNodeName := replicaNodeNames[i]
		if targetNodeAddress == localHostAddr {
			continue
		}

		diffReader, err := collectDiffForTargetNode(targetNodeAddress, targetNodeName)
		if err != nil {
			if errors.Is(err, ErrHashtreeRootUnchanged) {
				// Case A: this target hasn't flushed its memtable yet. Continue
				// checking remaining targets — they may have real diffs that would
				// be starved if we returned here. The skip state is per-target and
				// stored in the caller's map, so continuing does not affect it.
				hadUnchanged = true
				continue
			}
			if !errors.Is(err, ErrNoDiffFound) {
				ec.Add(err)
			}
			continue
		}

		return diffReader, nil
	}

	err = ec.ToError()
	if err != nil {
		return nil, err
	}

	// No target had real diffs. If at least one target was in Case A (remote
	// hasn't flushed yet), signal the caller to keep the fast-poll cadence.
	if hadUnchanged {
		return &ShardDifferenceReader{}, ErrHashtreeRootUnchanged
	}

	return &ShardDifferenceReader{}, ErrNoDiffFound
}

// expandDiscriminantToLeaves expands every non-leaf set bit in diff to its two
// leaf-level descendants, working top-down level by level. After stopping the
// comparison loop at height-1 (to skip the largest HashTreeLevel response),
// the discriminant has bits set at internal-node positions. HashTreeDiffReader
// only scans leaf positions, so this expansion is required before constructing
// the RangeReader.
//
// False positives are possible (both leaves under a differing parent are marked
// even if only one truly differs), but DigestObjectsInRange handles the
// fine-grained reconciliation, and propagation is idempotent.
func expandDiscriminantToLeaves(diff *hashtree.Bitset, height int) {
	for l := 0; l < height; l++ {
		offset := hashtree.InnerNodesCount(l)
		count := hashtree.LeavesCount(l)
		for j := 0; j < count; j++ {
			node := offset + j
			if diff.IsSet(node) {
				diff.Set(2*node + 1)
				diff.Set(2*node + 2)
				diff.Unset(node)
			}
		}
	}
}

func (f *Finder) DigestObjectsInRange(ctx context.Context,
	shardName string, host string, initialUUID, finalUUID strfmt.UUID, limit int,
) (ds []types.RepairResponse, err error) {
	return f.client.DigestObjectsInRange(ctx, host, f.class, shardName, initialUUID, finalUUID, limit)
}

// CompareDigests sends the source's local digests to the target node and
// returns a subset requiring source-side action. The target returns an entry
// for an object only when:
//   - the object is absent on the target (missing), or
//   - the source has a strictly newer UpdateTime than the target (target is stale), or
//   - the target holds a tombstone for the object (Deleted=true), with the
//     deletion verdict determined by the collection's DeletionStrategy.
//
// Equal-timestamp objects are never returned: because the hashtree digest is
// hash(uuid, updateTime), two nodes holding the same UpdateTime for an object
// produce identical digests and are invisible to the hashtree diff. There is
// therefore no equal-timestamp conflict path in this protocol.
func (f *Finder) CompareDigests(ctx context.Context,
	shardName string, host string, digests []types.RepairResponse,
) ([]types.RepairResponse, error) {
	return f.client.CompareDigests(ctx, host, f.class, shardName, digests)
}

// Overwrite specified object with most recent contents
func (f *Finder) Overwrite(ctx context.Context,
	host, index, shard string, xs []*objects.VObject,
) ([]types.RepairResponse, error) {
	return f.client.Overwrite(ctx, host, index, shard, xs)
}

func (f *Finder) LocalNodeName() string {
	return f.nodeName
}

// AsyncCheckpointNodeStatus holds the checkpoint state of one replica for one shard.
// If CutoffMs > 0 the replica has an active checkpoint and Root is the bounded-tree
// root. If CutoffMs == 0 the replica has no active checkpoint and Root is zero
// (the batch status API does not return the unbounded root for inactive checkpoints).
type AsyncCheckpointNodeStatus struct {
	Node      string
	CutoffMs  int64
	CreatedAt time.Time
	Root      hashtree.Digest
}

// AsyncCheckpointShardStatus is the per-shard checkpoint state as returned by a
// single node. It omits the node name; callers supply it when building
// AsyncCheckpointNodeStatus entries.
type AsyncCheckpointShardStatus struct {
	Root      hashtree.Digest
	CutoffMs  int64
	CreatedAt time.Time
}

// remoteReplicaHosts returns the host addresses (and node names) of all non-local
// replicas for the given shard. Replicas whose hostname cannot be resolved are skipped.
func (f *Finder) remoteReplicaHosts(shardName string) (names []string, addrs []string) {
	options := f.router.BuildRoutingPlanOptions(shardName, shardName, types.ConsistencyLevelOne, "")
	routingPlan, err := f.router.BuildReadRoutingPlan(options)
	if err != nil {
		return nil, nil
	}
	localAddr, localResolved := f.nodeResolver.NodeHostname(f.nodeName)
	for _, nodeName := range routingPlan.NodeNames() {
		// Always skip the local node by name; fall back to addr comparison only
		// when local addr resolution succeeded (avoids treating self as remote
		// when NodeHostname returns "" for the local node).
		if nodeName == f.nodeName {
			continue
		}
		addr, ok := f.nodeResolver.NodeHostname(nodeName)
		if !ok || (localResolved && addr == localAddr) {
			continue
		}
		names = append(names, nodeName)
		addrs = append(addrs, addr)
	}
	return names, addrs
}

// BroadcastCreateAsyncCheckpoint fans out a batch create to all non-local
// replicas of the given shards. It groups shards by remote replica node address
// and issues one request per node, rather than one request per shard.
// Individual node failures are silently ignored.
func (f *Finder) BroadcastCreateAsyncCheckpoint(ctx context.Context, shardNames []string, cutoffMs int64, createdAt time.Time) {
	addrShards := f.groupShardsByAddr(shardNames)
	for addr, shards := range addrShards {
		_ = f.client.CreateAsyncCheckpoint(ctx, addr, f.class, shards, cutoffMs, createdAt)
	}
}

// BroadcastDeleteAsyncCheckpoint fans out a batch delete to all non-local
// replicas of the given shards. It groups shards by remote replica node address
// and issues one request per node, rather than one request per shard.
// Individual node failures are silently ignored.
func (f *Finder) BroadcastDeleteAsyncCheckpoint(ctx context.Context, shardNames []string) {
	addrShards := f.groupShardsByAddr(shardNames)
	for addr, shards := range addrShards {
		_ = f.client.DeleteAsyncCheckpoint(ctx, addr, f.class, shards)
	}
}

// groupShardsByAddr builds a map from remote replica address to the subset of
// shardNames for which that address is a non-local replica.
func (f *Finder) groupShardsByAddr(shardNames []string) map[string][]string {
	addrShards := make(map[string][]string)
	for _, shardName := range shardNames {
		_, addrs := f.remoteReplicaHosts(shardName)
		for _, addr := range addrs {
			addrShards[addr] = append(addrShards[addr], shardName)
		}
	}
	return addrShards
}

// BroadcastGetAsyncCheckpointStatus queries all non-local replicas for checkpoint
// state for the given shards. Shards are grouped by remote node so one request is
// issued per node rather than one per shard. Unreachable nodes are silently skipped.
// Returns map[shardName][]AsyncCheckpointNodeStatus with entries from all
// reachable remote replicas.
func (f *Finder) BroadcastGetAsyncCheckpointStatus(ctx context.Context, shardNames []string) map[string][]AsyncCheckpointNodeStatus {
	addrShards := make(map[string][]string)
	addrToName := make(map[string]string)
	for _, shardName := range shardNames {
		names, addrs := f.remoteReplicaHosts(shardName)
		for i, addr := range addrs {
			addrShards[addr] = append(addrShards[addr], shardName)
			addrToName[addr] = names[i]
		}
	}

	out := make(map[string][]AsyncCheckpointNodeStatus)
	for addr, shards := range addrShards {
		nodeName := addrToName[addr]
		statuses, err := f.client.GetAsyncCheckpointStatus(ctx, addr, f.class, shards)
		if err != nil {
			continue
		}
		for shardName, s := range statuses {
			out[shardName] = append(out[shardName], AsyncCheckpointNodeStatus{
				Node:      nodeName,
				CutoffMs:  s.CutoffMs,
				CreatedAt: s.CreatedAt,
				Root:      s.Root,
			})
		}
	}
	return out
}
