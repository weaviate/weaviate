//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package hnsw

// NoopCommitLogger implements the CommitLogger interface, but does not
// actually write anything to disk
type NoopCommitLogger struct{}

func (n *NoopCommitLogger) AddNode(node *vertex) error {
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

func (n *NoopCommitLogger) Reset() error {
	return nil
}

func (n *NoopCommitLogger) Drop() error {
	return nil
}

func MakeNoopCommitLogger() (CommitLogger, error) {
	return &NoopCommitLogger{}, nil
}

func (b *NoopCommitLogger) NewBufferedLinksLogger() BufferedLinksLogger {
	return b // return self as it does not do anything anyway
}

func (b *NoopCommitLogger) Close() error {
	return nil
}
