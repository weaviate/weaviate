package classification

import (
	"encoding/json"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/usecases/cluster"
)

const TransactionPut cluster.TransactionType = "put_single"

type TransactionPutPayload struct {
	Classification models.Classification `json:"classification"`
}

func UnmarshalTransaction(txType cluster.TransactionType,
	payload json.RawMessage) (interface{}, error) {
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
