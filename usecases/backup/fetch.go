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
	"strings"
	"sync"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/backup"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// FetchBackupDescriptors fetches and unmarshals backup descriptors concurrently.
// keys are the object names/paths to fetch; only keys ending with GlobalBackupFile
// are processed. fetch is called for each matching key to retrieve the raw JSON bytes.
func FetchBackupDescriptors(
	ctx context.Context,
	logger logrus.FieldLogger,
	keys []string,
	fetch func(ctx context.Context, key string) ([]byte, error),
) ([]*backup.DistributedBackupDescriptor, error) {
	var filteredKeys []string
	for _, k := range keys {
		if strings.HasSuffix(k, GlobalBackupFile) {
			filteredKeys = append(filteredKeys, k)
		}
	}
	if len(filteredKeys) == 0 {
		return nil, nil
	}

	eg, ctx := enterrors.NewErrorGroupWithContextWrapper(logger, ctx)
	eg.SetLimit(runtime.GOMAXPROCS(0) * 2)
	mu := &sync.Mutex{}
	meta := make([]*backup.DistributedBackupDescriptor, 0, len(filteredKeys))

	for _, key := range filteredKeys {
		eg.Go(func() error {
			contents, err := fetch(ctx, key)
			if err != nil {
				return fmt.Errorf("fetch descriptor %q: %w", key, err)
			}
			var desc backup.DistributedBackupDescriptor
			if err := json.Unmarshal(contents, &desc); err != nil {
				return fmt.Errorf("unmarshal descriptor %q: %w", key, err)
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
