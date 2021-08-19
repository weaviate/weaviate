package schema

import (
	"encoding/json"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/usecases/cluster"
	"github.com/semi-technologies/weaviate/usecases/sharding"
)

const (
	AddClass    cluster.TransactionType = "add_class"
	AddProperty cluster.TransactionType = "add_property"
	DeleteClass cluster.TransactionType = "delete_class"
)

type AddClassPayload struct {
	Class *models.Class   `json:"class"`
	State *sharding.State `json:"state"`
}

type AddPropertyPayload struct {
	ClassName string           `json:"className"`
	Property  *models.Property `json:"property"`
}

type DeleteClassPayload struct {
	ClassName string `json:"className"`
}

func UnmarshalTransaction(txType cluster.TransactionType,
	payload json.RawMessage) (interface{}, error) {
	switch txType {
	case AddClass:
		return unmarshalAddClass(payload)

	case AddProperty:
		return unmarshalAddProperty(payload)

	case DeleteClass:
		return unmarshalDeleteClass(payload)

	default:
		return nil, errors.Errorf("unrecognized schema transaction type %q", txType)

	}
}

func unmarshalAddClass(payload json.RawMessage) (interface{}, error) {
	var pl AddClassPayload
	if err := json.Unmarshal(payload, &pl); err != nil {
		return nil, err
	}

	return pl, nil
}

func unmarshalAddProperty(payload json.RawMessage) (interface{}, error) {
	var pl AddPropertyPayload
	if err := json.Unmarshal(payload, &pl); err != nil {
		return nil, err
	}

	return pl, nil
}

func unmarshalDeleteClass(payload json.RawMessage) (interface{}, error) {
	var pl DeleteClassPayload
	if err := json.Unmarshal(payload, &pl); err != nil {
		return nil, err
	}

	return pl, nil
}
