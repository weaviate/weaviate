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

package schema

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/usecases/cluster"
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
			if !Equal(previous, current) {
				diff := Diff("previous", previous, "current", current)
				logger.WithFields(logrusStartupSyncFields()).WithFields(logrus.Fields{
					"diff": diff,
				}).Errorf("trying to reach cluster consensus on schema")

				return nil, fmt.Errorf("did not reach consensus on schema in cluster")
			}
		}

		return consensus, nil
	}
}

// Equal compares two schema states for equality
// First the object classes are sorted, because
// they are unordered. Then we can make the comparison
// using DeepEqual
func Equal(first, second *State) bool {
	if first.ObjectSchema == nil && second.ObjectSchema == nil {
		return true
	} else if first.ObjectSchema == nil || second.ObjectSchema == nil {
		return false
	}

	if len(first.ObjectSchema.Classes) != len(second.ObjectSchema.Classes) {
		return false
	}

	if len(first.ObjectSchema.Classes) != 0 && len(second.ObjectSchema.Classes) != 0 {
		sort.Slice(first.ObjectSchema.Classes, func(i, j int) bool {
			return first.ObjectSchema.Classes[i].Class < first.ObjectSchema.Classes[j].Class
		})

		sort.Slice(second.ObjectSchema.Classes, func(i, j int) bool {
			return second.ObjectSchema.Classes[i].Class < second.ObjectSchema.Classes[j].Class
		})
	}

	return reflect.DeepEqual(first, second)
}
