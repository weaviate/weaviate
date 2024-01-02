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

package hnsw

import (
	"context"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
)

// NoopCommitLogger implements the CommitLogger interface, but does not
// actually write anything to disk
type NoopCommitLogger struct{}

func (n *NoopCommitLogger) ID() string {
	return ""
}

func (n *NoopCommitLogger) AddPQ(data compressionhelpers.PQData) error {
	return nil
}

func (n *NoopCommitLogger) AddNode(node *vertex) error {
	return nil
}

func (n *NoopCommitLogger) Flush() error {
	return nil
}

func (n *NoopCommitLogger) SetEntryPointWithMaxLayer(id uint64, level int) error {
	return nil
}

func (n *NoopCommitLogger) AddLinkAtLevel(nodeid uint64, level int, target uint64) error {
	return nil
}

func (n *NoopCommitLogger) ReplaceLinksAtLevel(nodeid uint64, level int, targets []uint64) error {
	return nil
}

func (n *NoopCommitLogger) AddTombstone(nodeid uint64) error {
	return nil
}

func (n *NoopCommitLogger) RemoveTombstone(nodeid uint64) error {
	return nil
}

func (n *NoopCommitLogger) DeleteNode(nodeid uint64) error {
	return nil
}

func (n *NoopCommitLogger) ClearLinks(nodeid uint64) error {
	return nil
}

func (n *NoopCommitLogger) ClearLinksAtLevel(nodeid uint64, level uint16) error {
	return nil
}

func (n *NoopCommitLogger) Reset() error {
	return nil
}

func (n *NoopCommitLogger) Drop(ctx context.Context) error {
	return nil
}

func (n *NoopCommitLogger) Shutdown(context.Context) error {
	return nil
}

func MakeNoopCommitLogger() (CommitLogger, error) {
	return &NoopCommitLogger{}, nil
}

func (n *NoopCommitLogger) NewBufferedLinksLogger() BufferedLinksLogger {
	return n // return self as it does not do anything anyway
}

func (n *NoopCommitLogger) Close() error {
	return nil
}

func (n *NoopCommitLogger) StartSwitchLogs() chan struct{} {
	return make(chan struct{})
}

func (n *NoopCommitLogger) RootPath() string {
	return ""
}

func (n *NoopCommitLogger) SwitchCommitLogs(force bool) error {
	return nil
}
