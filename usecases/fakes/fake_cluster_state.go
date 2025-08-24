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

package fakes

import (
	command "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/cluster/mocks"
)

type FakeClusterState struct {
	cluster.NodeSelector
	syncIgnored bool
	skipRepair  bool
}

func NewFakeClusterState(hosts ...string) *FakeClusterState {
	if len(hosts) == 0 {
		hosts = []string{"node-1"}
	}

	return &FakeClusterState{
		NodeSelector: mocks.NewMockNodeSelector(hosts...),
	}
}

func (f *FakeClusterState) SchemaSyncIgnored() bool {
	return f.syncIgnored
}

func (f *FakeClusterState) SkipSchemaRepair() bool {
	return f.skipRepair
}

func (f *FakeClusterState) Hostnames() []string {
	return f.StorageCandidates()
}

func (f *FakeClusterState) AllNames() []string {
	return f.StorageCandidates()
}

func (f *FakeClusterState) LocalName() string {
	return "node1"
}

func (f *FakeClusterState) NodeCount() int {
	return 1
}

func (f *FakeClusterState) ClusterHealthScore() int {
	return 0
}

func (f *FakeClusterState) ResolveParentNodes(string, string,
) (map[string]string, error) {
	return nil, nil
}

func (f *FakeClusterState) NodeHostname(host string) (string, bool) {
	return f.NodeSelector.NodeHostname(host)
}

func (f *FakeClusterState) Execute(cmd *command.ApplyRequest) error {
	return nil
}
