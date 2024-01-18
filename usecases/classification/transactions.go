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

package classification

import (
	"encoding/json"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/cluster"
)

const TransactionPut cluster.TransactionType = "put_single"

type TransactionPutPayload struct {
	Classification models.Classification `json:"classification"`
}

func UnmarshalTransaction(txType cluster.TransactionType,
	payload json.RawMessage,
) (interface{}, error) {
	switch txType {
	case TransactionPut:
		return unmarshalPut(payload)

	default:
		return nil, errors.Errorf("unrecognized schema transaction type %q", txType)

	}
}

func unmarshalPut(payload json.RawMessage) (interface{}, error) {
	var pl TransactionPutPayload
	if err := json.Unmarshal(payload, &pl); err != nil {
		return nil, err
	}

	return pl, nil
}
