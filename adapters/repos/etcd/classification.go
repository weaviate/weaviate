//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Holding B.V. (registered @ Dutch Chamber of Commerce no 75221632). All rights reserved.
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
	"github.com/go-openapi/strfmt"
	"github.com/semi-technologies/weaviate/entities/models"
)

// ClassificationStorageKey is the etcd key used to store the schema
const ClassificationStorageKey = "/weaviate/classifications"

func classificationKeyFromID(id strfmt.UUID) string {
	return fmt.Sprintf("%s/%s", ClassificationStorageKey, id)
}

// SchemaRepo is an etcd-based Repo to load and persist schema changes
type ClassificationRepo struct {
	client *clientv3.Client
}

// NewClassificationRepo based on etcd
func NewClassificationRepo(client *clientv3.Client) *ClassificationRepo {
	return &ClassificationRepo{
		client: client,
	}
}

// Put classification in remote repository
func (r *ClassificationRepo) Put(ctx context.Context, classification models.Classification) error {
	stateBytes, err := json.Marshal(classification)
	if err != nil {
		return fmt.Errorf("could not marshal classification to json: %s", err)
	}

	_, err = r.client.Put(ctx, classificationKeyFromID(classification.ID), string(stateBytes))
	if err != nil {
		return fmt.Errorf("could not store classification in etcd: %s", err)
	}

	return nil
}

// Get returns the classification if a previous version has been stored, or nil
// to indicated that no previous classification had been stored.
func (r *ClassificationRepo) Get(ctx context.Context, id strfmt.UUID) (*models.Classification, error) {
	res, err := r.client.Get(ctx, classificationKeyFromID(id))
	if err != nil {
		return nil, fmt.Errorf("could not retrieve key '%s' from etcd: %v",
			classificationKeyFromID(id), err)
	}

	switch k := len(res.Kvs); {
	case k == 0:
		return nil, nil
	case k == 1:
		return r.unmarshalClassification(res.Kvs[0].Value)
	default:
		return nil, fmt.Errorf("unexpected number of results for key '%s', "+
			"expected to have 0 or 1, but got %d: %#v", classificationKeyFromID(id),
			len(res.Kvs), res.Kvs)
	}
}

func (r *ClassificationRepo) unmarshalClassification(bytes []byte) (*models.Classification, error) {
	var class models.Classification
	err := json.Unmarshal(bytes, &class)
	if err != nil {
		return nil, fmt.Errorf("could not parse the classification: %s", err)
	}

	return &class, nil
}
