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
	"encoding/json"
	"time"

	"github.com/pkg/errors"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/usecases/cluster"
	"github.com/semi-technologies/weaviate/usecases/sharding"
)

const (
	// write-only
	AddClass    cluster.TransactionType = "add_class"
	AddProperty cluster.TransactionType = "add_property"
	DeleteClass cluster.TransactionType = "delete_class"
	UpdateClass cluster.TransactionType = "update_class"

	// read-only
	ReadSchema cluster.TransactionType = "read_schema"

	DefaultTxTTL = 60 * time.Second
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

type UpdateClassPayload struct {
	ClassName string        `json:"className"`
	Class     *models.Class `json:"class"`

	// For now, the state cannot be updated yet, but this will be a requirement
	// in the future, for example, with dynamically changing replication, so we
	// should already make sure that state is part of the transaction payload
	State *sharding.State `json:"state"`
}

type ReadSchemaPayload struct {
	Schema *State `json:"schema"`
}

func UnmarshalTransaction(txType cluster.TransactionType,
	payload json.RawMessage,
) (interface{}, error) {
	switch txType {

	case AddClass:
		return unmarshalAddClass(payload)

	case AddProperty:
		return unmarshalAddProperty(payload)

	case DeleteClass:
		return unmarshalDeleteClass(payload)

	case UpdateClass:
		return unmarshalUpdateClass(payload)

	case ReadSchema:
		return unmarshalReadSchema(payload)

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

func unmarshalUpdateClass(payload json.RawMessage) (interface{}, error) {
	var pl UpdateClassPayload
	if err := json.Unmarshal(payload, &pl); err != nil {
		return nil, err
	}

	return pl, nil
}

func unmarshalReadSchema(payload json.RawMessage) (interface{}, error) {
	var pl ReadSchemaPayload
	if err := json.Unmarshal(payload, &pl); err != nil {
		return nil, err
	}

	return pl, nil
}
