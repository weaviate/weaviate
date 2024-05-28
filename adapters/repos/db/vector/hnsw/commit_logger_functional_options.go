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

import "github.com/weaviate/weaviate/usecases/memwatch"

type CommitlogOption func(l *hnswCommitLogger) error

func WithCommitlogThreshold(size int64) CommitlogOption {
	return func(l *hnswCommitLogger) error {
		l.maxSizeIndividual = size
		return nil
	}
}

func WithCommitlogThresholdForCombining(size int64) CommitlogOption {
	return func(l *hnswCommitLogger) error {
		l.maxSizeCombining = size
		return nil
	}
}

func WithAllocChecker(mm memwatch.AllocChecker) CommitlogOption {
	return func(l *hnswCommitLogger) error {
		l.allocChecker = mm
		return nil
	}
}

func WithCondensor(condensor Condensor) CommitlogOption {
	return func(l *hnswCommitLogger) error {
		l.condensor = condensor
		return nil
	}
}
