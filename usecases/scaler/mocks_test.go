//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package scaler

import (
	"context"
	"errors"
	"io"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/cluster/mocks"
	"github.com/weaviate/weaviate/usecases/sharding"
)

const (
	localNode = "N1"
)

var (
	anyVal = mock.Anything
	errAny = errors.New("any error")
)

type fakeFactory struct {
	LocalNode     string
	Nodes         []string
	ShardingState fakeShardingState
	NodeHostMap   map[string]string
	Source        *fakeSource
	Client        *fakeClient
	logger        logrus.FieldLogger
}

func newFakeFactory() *fakeFactory {
	nodeHostMap := map[string]string{"N1": "H1", "N2": "H2", "N3": "H3", "N4": "H4"}
	nodes := []string{"N1", "N2", "N3", "N4"}
	logger, _ := test.NewNullLogger()
	return &fakeFactory{
		LocalNode: localNode,
		Nodes:     nodes,
		ShardingState: fakeShardingState{
			LocalNode: localNode,
			M: map[string][]string{
				"S1": {"N1"},
				"S3": {"N3", "N4"},
			},
		},
		NodeHostMap: nodeHostMap,
		Source:      &fakeSource{},
		Client:      &fakeClient{},
		logger:      logger,
	}
}

func (f *fakeFactory) Scaler(dataPath string) *Scaler {
	nodeResolver := newFakeNodeResolver(f.LocalNode, f.NodeHostMap)
	scaler := New(
		nodeResolver,
		f.Source,
		f.Client,
		f.logger,
		dataPath)
	scaler.SetSchemaReader(&f.ShardingState)
	return scaler
}

type fakeShardingState struct {
	LocalNode string
	M         map[string][]string
}

func (f *fakeShardingState) CopyShardingState(class string) *sharding.State {
	if len(f.M) == 0 {
		return nil
	}
	state := sharding.State{}
	state.Physical = make(map[string]sharding.Physical)
	for shard, nodes := range f.M {
		state.Physical[shard] = sharding.Physical{BelongsToNodes: nodes}
	}
	state.SetLocalName(f.LocalNode)
	return &state
}

// node resolver
type fakeNodeResolver struct {
	cluster.NodeSelector
	NodeName string
	M        map[string]string
}

func newFakeNodeResolver(localNode string, nodeHostMap map[string]string) *fakeNodeResolver {
	var names []string
	for name := range nodeHostMap {
		names = append(names, name)
	}
	return &fakeNodeResolver{NodeName: localNode, M: nodeHostMap, NodeSelector: mocks.NewMockNodeSelector(names...)}
}

// NodeHostname needed to override the common cluster.NodeSelector
func (r *fakeNodeResolver) NodeHostname(nodeName string) (string, bool) {
	host, ok := r.M[nodeName]
	return host, ok
}

type fakeSource struct {
	mock.Mock
}

func (s *fakeSource) ReleaseBackup(ctx context.Context, id, class string) error {
	args := s.Called(ctx, id, class)
	return args.Error(0)
}

func (s *fakeSource) ShardsBackup(
	ctx context.Context, id, class string, shards []string,
) (_ backup.ClassDescriptor, err error) {
	args := s.Called(ctx, id, class, shards)
	return args.Get(0).(backup.ClassDescriptor), args.Error(1)
}

type fakeClient struct {
	mock.Mock
}

func (f *fakeClient) PutFile(ctx context.Context, host, class,
	shard, filename string, payload io.ReadSeekCloser,
) error {
	args := f.Called(ctx, host, class, shard, filename, payload)
	return args.Error(0)
}

func (f *fakeClient) CreateShard(ctx context.Context, host, class, name string) error {
	args := f.Called(ctx, host, class, name)
	return args.Error(0)
}

func (f *fakeClient) ReInitShard(ctx context.Context, host, class, shard string,
) error {
	args := f.Called(ctx, host, class, shard)
	return args.Error(0)
}

func (f *fakeClient) IncreaseReplicationFactor(ctx context.Context,
	host, class string, dist ShardDist,
) error {
	args := f.Called(ctx, host, class, dist)
	return args.Error(0)
}
