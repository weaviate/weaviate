package schema

import (
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
