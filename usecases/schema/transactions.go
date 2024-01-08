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
	AddClass            cluster.TransactionType = "add_class"
	AddProperty         cluster.TransactionType = "add_property"
	mergeObjectProperty cluster.TransactionType = "merge_object_property"

	// tenant types
	addTenants    cluster.TransactionType = "add_tenants"
	updateTenants cluster.TransactionType = "update_tenants"
	deleteTenants cluster.TransactionType = "delete_tenants"

	DeleteClass cluster.TransactionType = "delete_class"
	UpdateClass cluster.TransactionType = "update_class"

	// read-only
	ReadSchema cluster.TransactionType = "read_schema"

	// repairs
	RepairClass    cluster.TransactionType = "repair_class"
	RepairProperty cluster.TransactionType = "repair_property"
	RepairTenant   cluster.TransactionType = "repair_tenant"

	DefaultTxTTL = 60 * time.Second
)

// any tx that is listed here will be tried to commit if it is still open on
// startup
var resumableTxs = []cluster.TransactionType{
	addTenants,
}

// any tx that is listed here will bypass the ready-check, i.e. they will
// execute even if the DB is unready.
var allowUnreadyTxs = []cluster.TransactionType{
	ReadSchema, // required at startup, does not write
	RepairClass,
	RepairProperty,
	RepairTenant,
}

type AddClassPayload struct {
	Class *models.Class   `json:"class"`
	State *sharding.State `json:"state"`
}

type AddPropertyPayload struct {
	ClassName string           `json:"className"`
	Property  *models.Property `json:"property"`
}

type MergeObjectPropertyPayload struct {
	ClassName string           `json:"className"`
	Property  *models.Property `json:"property"`
}

// TenantCreate represents properties of a specific tenant (physical shard)
type TenantCreate struct {
	Name   string   `json:"name"`
	Nodes  []string `json:"nodes"`
	Status string   `json:"status"`
}

type TenantUpdate struct {
	Name   string `json:"name"`
	Status string `json:"status"`
}

// AddTenantsPayload allows for adding multiple tenants to a class
type AddTenantsPayload struct {
	Class   string         `json:"class_name"`
	Tenants []TenantCreate `json:"tenants"`
}

type UpdateTenantsPayload struct {
	Class   string         `json:"class_name"`
	Tenants []TenantUpdate `json:"tenants"`
}

// DeleteTenantsPayload allows for removing multiple tenants from a class
type DeleteTenantsPayload struct {
	Class   string   `json:"class_name"`
	Tenants []string `json:"tenants"`
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
	case AddClass, RepairClass:
		return unmarshalRawJson[AddClassPayload](payload)
	case AddProperty, RepairProperty:
		return unmarshalRawJson[AddPropertyPayload](payload)
	case mergeObjectProperty:
		return unmarshalRawJson[MergeObjectPropertyPayload](payload)
	case DeleteClass:
		return unmarshalRawJson[DeleteClassPayload](payload)
	case UpdateClass:
		return unmarshalRawJson[UpdateClassPayload](payload)
	case ReadSchema:
		return unmarshalRawJson[ReadSchemaPayload](payload)
	case addTenants, RepairTenant:
		return unmarshalRawJson[AddTenantsPayload](payload)
	case updateTenants:
		return unmarshalRawJson[UpdateTenantsPayload](payload)
	case deleteTenants:
		return unmarshalRawJson[DeleteTenantsPayload](payload)
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
