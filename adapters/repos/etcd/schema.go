//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2019 Weaviate. All rights reserved.
//  LICENSE WEAVIATE OPEN SOURCE: https://www.semi.technology/playbook/playbook/contract-weaviate-OSS.html
//  LICENSE WEAVIATE ENTERPRISE: https://www.semi.technology/playbook/contract-weaviate-enterprise.html
//  CONCEPT: Bob van Luijt (@bobvanluijt)
//  CONTACT: hello@semi.technology
//

package etcd

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/coreos/etcd/clientv3"
	"github.com/semi-technologies/weaviate/usecases/schema"
)

// SchemaStateStorageKey is the etcd key used to store the schema
const SchemaStateStorageKey = "/weaviate/schema/state"

// SchemaRepo is an etcd-based Repo to load and persist schema changes
type SchemaRepo struct {
	client *clientv3.Client
}

// NewSchemaRepo based on etcd
func NewSchemaRepo(client *clientv3.Client) *SchemaRepo {
	return &SchemaRepo{
		client: client,
	}
}

// SaveSchema in remote repository
func (r *SchemaRepo) SaveSchema(ctx context.Context, schema schema.State) error {
	stateBytes, err := json.Marshal(schema)
	if err != nil {
		return fmt.Errorf("could not marshal schema state to json: %s", err)
	}

	_, err = r.client.Put(ctx, SchemaStateStorageKey, string(stateBytes))
	if err != nil {
		return fmt.Errorf("could not store schema state in etcd: %s", err)
	}

	return nil
}

// LoadSchema returns the schema if a previous version has been stored, or nil
// to indicated that no previous schema had been stored.
func (r *SchemaRepo) LoadSchema(ctx context.Context) (*schema.State, error) {
	res, err := r.client.Get(ctx, SchemaStateStorageKey)
	if err != nil {
		return nil, fmt.Errorf("could not retrieve key '%s' from etcd: %v",
			SchemaStateStorageKey, err)
	}

	switch k := len(res.Kvs); {
	case k == 0:
		return nil, nil
	case k == 1:
		return r.unmarshalSchema(res.Kvs[0].Value)
	default:
		return nil, fmt.Errorf("unexpected number of results for key '%s', "+
			"expected to have 0 or 1, but got %d: %#v", SchemaStateStorageKey,
			len(res.Kvs), res.Kvs)
	}
}

func (r *SchemaRepo) unmarshalSchema(bytes []byte) (*schema.State, error) {
	var state schema.State
	err := json.Unmarshal(bytes, &state)
	if err != nil {
		return nil, fmt.Errorf("could not parse the schema state: %s", err)
	}

	return &state, nil
}
