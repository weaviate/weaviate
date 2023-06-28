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
	"encoding/json"
	"time"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/cluster"
	"github.com/weaviate/weaviate/usecases/sharding"
)

const (
	// write-only
	AddClass    cluster.TransactionType = "add_class"
	AddProperty cluster.TransactionType = "add_property"
	// AddPartitions to a specific class
	AddPartitions cluster.TransactionType = "add_partitions"

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

// Partition represents properties of a specific partition (physical shard)
type Partition struct {
	Name  string   `json:"name"`
	Nodes []string `json:"nodes"`
}

// AddPartitionsPayload allows for adding multiple partitions to a class
type AddPartitionsPayload struct {
	ClassName  string      `json:"className"`
	Partitions []Partition `json:"partitions"`
}

type DeleteClassPayload struct {
	ClassName string `json:"className"`
	Force     bool   `json:"force"`
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
		return unmarshalRawJson[AddClassPayload](payload)
	case AddProperty:
		return unmarshalRawJson[AddPropertyPayload](payload)
	case DeleteClass:
		return unmarshalRawJson[DeleteClassPayload](payload)
	case UpdateClass:
		return unmarshalRawJson[UpdateClassPayload](payload)
	case ReadSchema:
		return unmarshalRawJson[ReadSchemaPayload](payload)
	case AddPartitions:
		return unmarshalRawJson[AddPartitionsPayload](payload)
	default:
		return nil, errors.Errorf("unrecognized schema transaction type %q", txType)

	}
}

// unmarshalRawJson returns the result of marshalling json payload
func unmarshalRawJson[T any](payload json.RawMessage) (T, error) {
	var v T
	err := json.Unmarshal(payload, &v)

	return v, err
}
