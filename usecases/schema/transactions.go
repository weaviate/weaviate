package schema

import (
	"encoding/json"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/usecases/cluster"
	"github.com/semi-technologies/weaviate/usecases/sharding"
)

const (
	AddClass cluster.TransactionType = "add_class"
)

type AddClassPayload struct {
	Class *models.Class   `json:"class"`
	State *sharding.State `json:"state"`
}

func UnmarshalTransaction(txType cluster.TransactionType,
	payload json.RawMessage) (interface{}, error) {
	switch txType {
	case AddClass:
		var pl AddClassPayload
		if err := json.Unmarshal(payload, &pl); err != nil {
			return nil, err
		}
		return pl, nil

	default:
		return nil, errors.Errorf("unrecognized schema transaction type %q", txType)

	}
}
