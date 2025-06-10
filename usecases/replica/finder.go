//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package replica

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"slices"
	"strings"
	"time"

	"github.com/weaviate/weaviate/entities/errorcompounder"
	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/filters"

	"github.com/go-openapi/strfmt"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/storobj"
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
	router       router
	nodeName     string
	finderStream // stream of objects
	// control the op backoffs in the coordinator's Pull
	coordinatorPullBackoffInitialInterval time.Duration
	coordinatorPullBackoffMaxElapsedTime  time.Duration

	rand *rand.Rand // random number generator for shufflings
}

// NewFinder constructs a new finder instance
func NewFinder(className string,
	router router,
	nodeName string,
	client RClient,
	l logrus.FieldLogger,
	coordinatorPullBackoffInitialInterval time.Duration,
	coordinatorPullBackoffMaxElapsedTime time.Duration,
	getDeletionStrategy func() string,
) *Finder {
	cl := FinderClient{client}
	return &Finder{
		router:   router,
		nodeName: nodeName,
		finderStream: finderStream{
			repairer: repairer{
				class:               className,
				getDeletionStrategy: getDeletionStrategy,
				client:              cl,
				logger:              l,
			},
			log: l,
		},
		coordinatorPullBackoffInitialInterval: coordinatorPullBackoffInitialInterval,
		coordinatorPullBackoffMaxElapsedTime:  coordinatorPullBackoffMaxElapsedTime,
		rand:                                  rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// GetOne gets object which satisfies the giving consistency
func (f *Finder) GetOne(ctx context.Context,
	l types.ConsistencyLevel, shard string,
	id strfmt.UUID,
	props search.SelectProperties,
	adds additional.Properties,
) (*storobj.Object, error) {
	c := newReadCoordinator[findOneReply](f, shard,
		f.coordinatorPullBackoffInitialInterval, f.coordinatorPullBackoffMaxElapsedTime, f.getDeletionStrategy())
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

func (f *Finder) FindUUIDs(ctx context.Context,
	className, shard string, filters *filters.LocalFilter, l types.ConsistencyLevel,
) (uuids []strfmt.UUID, err error) {
	c := newReadCoordinator[[]strfmt.UUID](f, shard,
		f.coordinatorPullBackoffInitialInterval, f.coordinatorPullBackoffMaxElapsedTime, f.getDeletionStrategy())

	op := func(ctx context.Context, host string, _ bool) ([]strfmt.UUID, error) {
		return f.client.FindUUIDs(ctx, host, f.class, shard, filters)
	}

	replyCh, _, err := c.Pull(ctx, l, op, "", 30*time.Second)
	if err != nil {
		f.log.WithField("op", "pull.one").Error(err)
		return nil, fmt.Errorf("%s %q: %w", MsgCLevel, l, ErrReplicas)
	}

	res := make(map[strfmt.UUID]struct{})

	for r := range replyCh {
		if r.Err != nil {
			f.logger.WithField("op", "finder.find_uuids").WithError(r.Err).Debug("error in reply channel")
			continue
		}

		for _, uuid := range r.Value {
			res[uuid] = struct{}{}
		}
	}

	uuids = make([]strfmt.UUID, 0, len(res))

	for uuid := range res {
		uuids = append(uuids, uuid)
	}

	return uuids, err
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
	for _, part := range cluster(createBatch(xs)) {
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
	c := newReadCoordinator[existReply](f, shard,
		f.coordinatorPullBackoffInitialInterval, f.coordinatorPullBackoffMaxElapsedTime, f.getDeletionStrategy())
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
	host, ok := f.router.NodeHostname(nodeName)
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
		c = newReadCoordinator[BatchReply](f, batch.Shard,
			f.coordinatorPullBackoffInitialInterval, f.coordinatorPullBackoffMaxElapsedTime, f.getDeletionStrategy())
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

type ShardDifferenceReader struct {
	TargetNodeName    string
	TargetNodeAddress string
	RangeReader       hashtree.AggregatedHashTreeRangeReader
}

// CollectShardDifferences collects the differences between the local node and the target nodes.
// It returns a ShardDifferenceReader that contains the differences and the target node name/address.
// If no differences are found, it returns ErrNoDiffFound.
// When ErrNoDiffFound is returned as the error, the returned *ShardDifferenceReader may exist
// and have some (but not all) of its fields set.
func (f *Finder) CollectShardDifferences(ctx context.Context,
	shardName string, ht hashtree.AggregatedHashTree, diffTimeoutPerNode time.Duration,
	targetNodeOverrides []additional.AsyncReplicationTargetNodeOverride,
) (diffReader *ShardDifferenceReader, err error) {
	routingPlan, err := f.router.BuildReadRoutingPlan(types.RoutingPlanBuildOptions{
		Collection:       f.class,
		Shard:            shardName,
		ConsistencyLevel: types.ConsistencyLevelOne,
	})
	if err != nil {
		return nil, fmt.Errorf("%w : class %q shard %q", err, f.class, shardName)
	}

	collectDiffForTargetNode := func(targetNodeAddress, targetNodeName string) (*ShardDifferenceReader, error) {
		ctx, cancel := context.WithTimeout(ctx, diffTimeoutPerNode)
		defer cancel()

		diff := hashtree.NewBitset(hashtree.NodesCount(ht.Height()))

		digests := make([]hashtree.Digest, hashtree.LeavesCount(ht.Height()))

		diff.Set(0) // init comparison at root level

		for l := 0; l <= ht.Height(); l++ {
			_, err := ht.Level(l, diff, digests)
			if err != nil {
				return nil, fmt.Errorf("%q: %w", targetNodeAddress, err)
			}

			levelDigests, err := f.client.HashTreeLevel(ctx, targetNodeAddress, f.class, shardName, l, diff)
			if err != nil {
				return nil, fmt.Errorf("%q: %w", targetNodeAddress, err)
			}
			if len(levelDigests) == 0 {
				// no differences were found
				break
			}

			levelDiffCount := hashtree.LevelDiff(l, diff, digests, levelDigests)
			if levelDiffCount == 0 {
				// no differences were found
				break
			}
		}

		if diff.SetCount() == 0 {
			return &ShardDifferenceReader{
				TargetNodeName:    targetNodeName,
				TargetNodeAddress: targetNodeAddress,
			}, ErrNoDiffFound
		}

		return &ShardDifferenceReader{
			TargetNodeName:    targetNodeName,
			TargetNodeAddress: targetNodeAddress,
			RangeReader:       ht.NewRangeReader(diff),
		}, nil
	}

	ec := errorcompounder.New()

	// If the caller provided a list of target node overrides, filter the replicas to only include
	// the relevant overrides so that we only "push" updates to the specified nodes.
	localNodeName := f.LocalNodeName()
	targetNodesToUse := slices.Clone(routingPlan.Replicas)
	if len(targetNodeOverrides) > 0 {
		targetNodesToUse = make([]string, 0, len(targetNodeOverrides))
		for _, override := range targetNodeOverrides {
			if override.SourceNode == localNodeName && override.CollectionID == f.class && override.ShardID == shardName {
				targetNodesToUse = append(targetNodesToUse, override.TargetNode)
			}
		}
	}

	replicaNodeNames := make([]string, 0, len(routingPlan.Replicas))
	replicasHostAddrs := make([]string, 0, len(routingPlan.ReplicasHostAddrs))
	for _, replica := range targetNodesToUse {
		replicaNodeNames = append(replicaNodeNames, replica)
		replicaHostAddr, ok := f.router.NodeHostname(replica)
		if ok {
			replicasHostAddrs = append(replicasHostAddrs, replicaHostAddr)
		}
	}

	// shuffle the replicas to randomize the order in which we look for differences
	if len(replicasHostAddrs) > 1 {
		f.rand.Shuffle(len(replicasHostAddrs), func(i, j int) {
			replicasHostAddrs[i], replicasHostAddrs[j] = replicasHostAddrs[j], replicasHostAddrs[i]
		})
	}

	localHostAddr, _ := f.router.NodeHostname(localNodeName)

	for i, targetNodeAddress := range replicasHostAddrs {
		targetNodeName := replicaNodeNames[i]
		if targetNodeAddress == localHostAddr {
			continue
		}

		diffReader, err := collectDiffForTargetNode(targetNodeAddress, targetNodeName)
		if err != nil {
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

	return &ShardDifferenceReader{}, ErrNoDiffFound
}

func (f *Finder) DigestObjectsInRange(ctx context.Context,
	shardName string, host string, initialUUID, finalUUID strfmt.UUID, limit int,
) (ds []types.RepairResponse, err error) {
	return f.client.DigestObjectsInRange(ctx, host, f.class, shardName, initialUUID, finalUUID, limit)
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
