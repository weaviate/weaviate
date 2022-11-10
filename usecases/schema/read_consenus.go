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
		// TODO: actually reach consensus
		if len(in) == 0 || in[0].Type != ReadSchema {
			return nil, nil
		}

		tx := in[0]

		typed, err := UnmarshalTransaction(tx.Type, tx.Payload.(json.RawMessage))
		if err != nil {
			return nil, fmt.Errorf("unmarshal tx: %w", err)
		}

		err = parser(ctx, typed.(ReadSchemaPayload).Schema)
		if err != nil {
			return nil, fmt.Errorf("parse schema %w", err)
		}

		tx.Payload = typed

		fmt.Printf("warning: faking consensus: %v\n", tx.Payload)
		return tx, nil
	}
}

// Equal checks if both schemas are the same by first marshalling, then
// comparing their byte-representation
func Equal(s1, s2 *State) bool {
	if len(s1.ObjectSchema.Classes) != len(s2.ObjectSchema.Classes) {
		return false
	}

	s1JSON, _ := json.Marshal(s1)
	s2JSON, _ := json.Marshal(s2)

	return bytes.Equal(s1JSON, s2JSON)
}
