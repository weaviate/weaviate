//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package backup

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"sync"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/backup"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// FetchBackupDescriptors fetches and unmarshals backup descriptors concurrently.
// keys are the object names/paths to fetch. fetch is called for each key to retrieve
// the raw JSON bytes of the descriptor.
func FetchBackupDescriptors(
	ctx context.Context,
	logger logrus.FieldLogger,
	keys []string,
	fetch func(ctx context.Context, key string) ([]byte, error),
) ([]*backup.DistributedBackupDescriptor, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	eg, ctx := enterrors.NewErrorGroupWithContextWrapper(logger, ctx)
	eg.SetLimit(runtime.GOMAXPROCS(0) * 2)
	mu := &sync.Mutex{}
	meta := make([]*backup.DistributedBackupDescriptor, 0, len(keys))

	for _, key := range keys {
		eg.Go(func() error {
			contents, err := fetch(ctx, key)
			if err != nil {
				return fmt.Errorf("read object %q: %w", key, err)
			}
			var desc backup.DistributedBackupDescriptor
			if err := json.Unmarshal(contents, &desc); err != nil {
				return fmt.Errorf("unmarshal object %q: %w", key, err)
			}
			mu.Lock()
			meta = append(meta, &desc)
			mu.Unlock()
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	return meta, nil
}
