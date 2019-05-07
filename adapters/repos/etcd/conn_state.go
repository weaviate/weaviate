/*                          _       _
 *__      _____  __ ___   ___  __ _| |_ ___
 *\ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
 * \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
 *  \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
 *
 * Copyright © 2016 - 2019 Weaviate. All rights reserved.
 * LICENSE: https://github.com/semi-technologies/weaviate/blob/develop/LICENSE.md
 * DESIGN & CONCEPT: Bob van Luijt (@bobvanluijt)
 * CONTACT: hello@semi.technology
 */package etcd

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/coreos/etcd/clientv3"
)

// ConnectorStateStorageKey is the etcd key used to store the connector state
const ConnectorStateStorageKey = "/weaviate/connector/state"

// ConnStateRepo is an etcd-based Repo to load and persist schema changes
type ConnStateRepo struct {
	client *clientv3.Client
}

// NewConnStateRepo based on etcd
func NewConnStateRepo(client *clientv3.Client) *ConnStateRepo {
	return &ConnStateRepo{
		client: client,
	}
}

// Save in remote repository
func (r *ConnStateRepo) Save(ctx context.Context, state json.RawMessage) error {
	stateBytes, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("could not marshal connector state to json: %s", err)
	}

	_, err = r.client.Put(ctx, ConnectorStateStorageKey, string(stateBytes))
	if err != nil {
		return fmt.Errorf("could not store connector state in etcd: %s", err)
	}

	return nil
}

// Load returns the connector state if a previous version has been stored, or
// nil to indicated that no previous state had been stored.
func (r *ConnStateRepo) Load(ctx context.Context) (json.RawMessage, error) {
	res, err := r.client.Get(ctx, ConnectorStateStorageKey)
	if err != nil {
		return nil, fmt.Errorf("could not retrieve key '%s' from etcd: %v",
			ConnectorStateStorageKey, err)
	}

	switch k := len(res.Kvs); {
	case k == 0:
		return nil, nil
	case k == 1:
		return r.unmarshalState(res.Kvs[0].Value)
	default:
		return nil, fmt.Errorf("unexpected number of results for key '%s', "+
			"expected to have 0 or 1, but got %d: %#v", SchemaStateStorageKey,
			len(res.Kvs), res.Kvs)
	}
}

func (r *ConnStateRepo) unmarshalState(bytes []byte) (json.RawMessage, error) {
	var state json.RawMessage
	err := json.Unmarshal(bytes, &state)
	if err != nil {
		return nil, fmt.Errorf("could not parse the schema state: %s", err)
	}

	return state, nil
}
