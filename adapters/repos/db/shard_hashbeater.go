//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package db

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/usecases/replica"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
)

func (s *Shard) initHashBeater() {
	s.hashBeaterCtx, s.hashBeaterCancelFunc = context.WithCancel(context.Background())

	go func() {
		t := time.NewTicker(1 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-s.hashBeaterCtx.Done():
				return
			case <-t.C:
				err := s.hashBeat()
				if s.hashBeaterCtx.Err() != nil {
					return
				}
				if err != nil {
					s.index.logger.Printf("hashbeat at shard %s: %v", s.name, err)
					// TODO (jeroiraz): an exp-backoff delay may be convenient at this point
				}
			}
		}
	}()
}

func (s *Shard) hashBeat() error {
	// Note: a copy of the hashtree could be used if a more stable comparison is desired s.hashtree.Clone()
	// in such a case nodes may also need to have a stable copy of their corresponding hashtree.
	ht := s.hashtree

	// Note: any consistency level could be used
	replyCh, err := s.index.replicator.CollectShardDifferences(s.hashBeaterCtx, s.name, ht, replica.One, "")
	if err != nil {
		return fmt.Errorf("collecting differences: %w", err)
	}

	for r := range replyCh {
		if r.Err != nil {
			if !errors.Is(err, hashtree.ErrNoMoreDifferences) {
				s.index.logger.Printf("reading collected differences for shard %s: %v", s.name, r.Err)
			}
			continue
		}

		shardDiffReader := r.Value
		diffReader := shardDiffReader.DiffReader

		for {
			if s.hashBeaterCtx.Err() != nil {
				return s.hashBeaterCtx.Err()
			}

			initialToken, finalToken, err := diffReader.Next()
			if errors.Is(err, hashtree.ErrNoMoreDifferences) {
				break
			}
			if err != nil {
				return fmt.Errorf("difference reading: %w", err)
			}

			_, err = s.index.replicator.StepTowardsShardConsistency(
				s.hashBeaterCtx,
				s.name,
				shardDiffReader.Host,
				initialToken,
				finalToken,
			)
			if err != nil {
				s.index.logger.Printf("solving differences for shard %s: %v", s.name, r.Err)
				continue
			}
		}
	}

	return nil
}

func (s *Shard) stopHashBeater() {
	s.hashBeaterCancelFunc()
}
