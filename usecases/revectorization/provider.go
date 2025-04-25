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

package revectorization

import (
	"encoding/json"
	"fmt"

	"github.com/boltdb/bolt"
	"github.com/jonboulle/clockwork"
	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/usecases/objects"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
)

var bucketName = []byte("tasks")

type Provider struct {
	recorder           distributedtask.TaskCompletionRecorder
	metadataDB         *bolt.DB
	db                 *db.DB
	batchManager       *objects.BatchManager
	completionRecorder distributedtask.TaskCompletionRecorder
	schemaGetter       schemaUC.SchemaGetter
	clock              clockwork.Clock
	localNode          string
}

type ProviderParams struct {
	MetadataPath string
}

func NewProvider(params ProviderParams) (*Provider, error) {
	metadataDB, err := bolt.Open(params.MetadataPath, 0o600, &bolt.Options{})
	if err != nil {
		return nil, fmt.Errorf("opening boltdb: %v", err)
	}

	err = metadataDB.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketName)
		if err != nil {
			return fmt.Errorf("creating bucket: %v", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &Provider{
		metadataDB: metadataDB,
	}, nil
}

func (p *Provider) SetCompletionRecorder(recorder distributedtask.TaskCompletionRecorder) {
	p.recorder = recorder
}

func (p *Provider) GetLocalTasks() ([]distributedtask.TaskDescriptor, error) {
	return boltView(p.metadataDB, func(b *bolt.Bucket) ([]distributedtask.TaskDescriptor, error) {
		tasks, err := collectAll[LocalTaskState](b)
		if err != nil {
			return nil, fmt.Errorf("collecting tasks: %w", err)
		}

		return mapWithFn(tasks, func(t LocalTaskState) distributedtask.TaskDescriptor {
			return t.TaskDescriptor
		}), nil
	})
}

func (p *Provider) Close() error {
	return p.metadataDB.Close()
}

func (p *Provider) StartTask(distTask *distributedtask.Task) (distributedtask.TaskHandle, error) {
	task, err := newTask(p, distTask)
	if err != nil {
		return nil, fmt.Errorf("creating task: %w", err)
	}

	go task.Run()

	return task, nil
}

func (p *Provider) CleanupTask(desc distributedtask.TaskDescriptor) error {
	return p.metadataDB.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketName).Delete([]byte(desc.String()))
	})
}

func boltView[T any](db *bolt.DB, f func(b *bolt.Bucket) (T, error)) (T, error) {
	var result T

	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		if b == nil {
			return fmt.Errorf("bucket %s not found", string(bucketName))
		}
		res, err := f(b)
		if err != nil {
			return err
		}
		result = res
		return nil
	})
	if err != nil {
		var zero T
		return zero, err
	}

	return result, nil
}

func collectAll[T any](b *bolt.Bucket) ([]T, error) {
	var result []T
	if err := b.ForEach(func(_, v []byte) error {
		var item T
		if err := json.Unmarshal(v, &item); err != nil {
			return fmt.Errorf("unmarshalling %s: %w", string(v), err)
		}
		result = append(result, item)
		return nil
	}); err != nil {
		return nil, err
	}
	return result, nil
}

func mapWithFn[I, O any](slice []I, f func(t I) O) []O {
	result := make([]O, len(slice))
	for i, item := range slice {
		result[i] = f(item)
	}
	return result
}
