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

package schema

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/sharding"
)

type parserFn func(ctx context.Context, schema *State) error

func newReadConsensus(parser parserFn,
	logger logrus.FieldLogger,
) cluster.ConsensusFn {
	return func(ctx context.Context,
		in []*cluster.Transaction,
	) (*cluster.Transaction, error) {
		if len(in) == 0 || in[0].Type != ReadSchema {
			return nil, nil
		}

		var consensus *cluster.Transaction
		for i, tx := range in {

			typed, err := UnmarshalTransaction(tx.Type, tx.Payload.(json.RawMessage))
			if err != nil {
				return nil, fmt.Errorf("unmarshal tx: %w", err)
			}

			err = parser(ctx, typed.(ReadSchemaPayload).Schema)
			if err != nil {
				return nil, fmt.Errorf("parse schema %w", err)
			}

			if i == 0 {
				consensus = tx
				consensus.Payload = typed
				continue
			}

			if consensus.ID != tx.ID {
				return nil, fmt.Errorf("comparing txs with different IDs: %s vs %s",
					consensus.ID, tx.ID)
			}
			previous := consensus.Payload.(ReadSchemaPayload).Schema
			current := typed.(ReadSchemaPayload).Schema
			if err := Equal(previous, current); err != nil {
				diff := Diff("previous", previous, "current", current)
				logger.WithFields(logrusStartupSyncFields()).WithFields(logrus.Fields{
					"diff": diff,
				}).Errorf("trying to reach cluster consensus on schema: %v", err)

				return nil, fmt.Errorf("did not reach consensus on schema in cluster: %w", err)
			}
		}

		return consensus, nil
	}
}

// Equal compares two schema states for equality
// First the object classes are sorted, because
// they are unordered. Then we can make the comparison
// using DeepEqual
func Equal(lhs, rhs *State) error {
	if lhs == nil && rhs == nil {
		return nil
	}
	if lhs == nil || rhs == nil {
		return fmt.Errorf("nil state %p, %p", lhs, rhs)
	}
	if err := equalClasses(lhs.ObjectSchema, rhs.ObjectSchema); err != nil {
		return fmt.Errorf("class models mismatch: %w", err)
	}
	if err := equalSharding(lhs.ShardingState, rhs.ShardingState); err != nil {
		return fmt.Errorf("sharding state mismatch: %w", err)
	}
	return nil
}

func equalClasses(lhs, rhs *models.Schema) error {
	if lhs == nil && rhs == nil {
		return nil
	}
	if lhs == nil || rhs == nil {
		return fmt.Errorf("model mismatch: %p!=%p", lhs, rhs)
	}
	m, n := len(lhs.Classes), len(rhs.Classes)
	if n != m {
		return fmt.Errorf("class count mismatch: %d!=%d", m, n)
	}
	if m == 0 {
		return nil
	}
	// sort classes so we can compare them one by one
	sort.Slice(lhs.Classes, func(i, j int) bool {
		return lhs.Classes[i].Class < lhs.Classes[j].Class
	})

	sort.Slice(rhs.Classes, func(i, j int) bool {
		return rhs.Classes[i].Class < rhs.Classes[j].Class
	})

	for i, cls := range lhs.Classes {
		x := rhs.Classes[i]
		if !reflect.DeepEqual(cls, rhs.Classes[i]) {
			n1, n2 := "", ""
			if cls != nil {
				n1 = cls.Class
			}
			if x != nil {
				n2 = cls.Class
			}
			return fmt.Errorf("class mismatch at position %d: %s %s", i, n1, n2)
		}
	}

	return nil
}

func equalSharding(l, r map[string]*sharding.State) error {
	m, n := len(l), len(r)
	if m != n {
		return fmt.Errorf("class count mismatch: %d!=%d", m, n)
	}
	if m == 0 {
		return nil
	}
	for cls, u := range l {
		v := r[cls]
		if a, b := u.PartitioningEnabled, v.PartitioningEnabled; a != b {
			return fmt.Errorf("class %s: partitioning %t %t", cls, a, b)
		}
		if u.Config != v.Config {
			return fmt.Errorf("class %s: config mismatch", cls)
		}

		if nl, nr := len(u.Physical), len(v.Physical); nl != nr {
			return fmt.Errorf("class %s: number of physical shards: local=%d remote=%d", cls, nl, nr)
		}
		for k, lu := range u.Physical {
			if !reflect.DeepEqual(lu, v.Physical[k]) {
				return fmt.Errorf("class %q: physical shard %q", cls, k)
			}
		}

		if nl, nr := len(u.Virtual), len(v.Virtual); nl != nr {
			return fmt.Errorf("class %s: number of virtual shards: local=%d remote=%d", cls, nl, nr)
		}

		for i, lu := range u.Virtual {
			if !reflect.DeepEqual(lu, v.Virtual[i]) {
				return fmt.Errorf("class %s: virtual shard at position %d", cls, i)
			}
		}

	}
	return nil
}
