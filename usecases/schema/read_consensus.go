//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package schema

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/semi-technologies/weaviate/usecases/cluster"
)

type parserFn func(ctx context.Context, schema *State) error

func newReadConsensus(parser parserFn) cluster.ConsensusFn {
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

			if !Equal(consensus.Payload.(ReadSchemaPayload).Schema,
				typed.(ReadSchemaPayload).Schema) {
				return nil, fmt.Errorf("did not reach consensus on schema in cluster")
			}
		}

		return consensus, nil
	}
}

// Equal checks if both schemas are the same by first marshalling, then
// comparing their byte-representation
func Equal(s1, s2 *State) bool {
	s1JSON, _ := json.Marshal(s1)
	s2JSON, _ := json.Marshal(s2)

	return bytes.Equal(s1JSON, s2JSON)
}
