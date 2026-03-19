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
	"errors"
	"fmt"
	"strings"

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
	eg.SetLimit(100) // limit concurrency to 100 fetches at a time
	metaCh := make(chan *backup.DistributedBackupDescriptor)

	for _, key := range filteredKeys {
		eg.Go(func() error {
			contents, err := fetch(ctx, key)
			if err != nil {
				var notFoundErr backup.ErrNotFound
				if errors.As(err, &notFoundErr) {
					return nil // skip not found errors, treat as if no descriptor exists
				}
				return fmt.Errorf("fetch descriptor %q: %w", key, err)
			}
			var desc backup.DistributedBackupDescriptor
			if err := json.Unmarshal(contents, &desc); err != nil {
				return fmt.Errorf("unmarshal descriptor %q: %w", key, err)
			}
			metaCh <- &desc
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	close(metaCh)
	meta := make([]*backup.DistributedBackupDescriptor, 0, len(filteredKeys))
	for desc := range metaCh {
		meta = append(meta, desc)
	}
	if len(meta) == 0 {
		return nil, nil
	}
	return meta, nil
}
