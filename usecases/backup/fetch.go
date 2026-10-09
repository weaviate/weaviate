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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	eg.SetLimit(32)
	metaCh := make(chan *backup.DistributedBackupDescriptor, len(filteredKeys))

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

// maxPresize caps the buffer ReadAllSized allocates up front at 64 MiB. The size
// is the backend's claim, and a buggy S3-compatible store or a proxy can overstate it.
const maxPresize = 64 << 20

// ReadAllSized reads r to EOF into a buffer allocated once for size bytes, the
// object length the backend reported, so the buffer does not double as it fills.
// Above maxPresize the buffer starts at maxPresize and then doubles, but never
// past size, so a 65 MiB object does not end up in a 128 MiB buffer. A negative
// size means the length is unknown.
func ReadAllSized(r io.Reader, size int64) ([]byte, error) {
	if size < 0 {
		return io.ReadAll(r)
	}
	// The MinRead spare bytes leave room for the Read that returns io.EOF, so an
	// object of size bytes never makes the loop grow the buffer.
	b := make([]byte, 0, min(size, maxPresize)+bytes.MinRead)
	for {
		if len(b) == cap(b) {
			newCap := 2 * int64(cap(b))
			// size+MinRead overflows for a size near MaxInt64, so the check subtracts from newCap.
			if int64(len(b)) < size && size < newCap-bytes.MinRead {
				newCap = size + bytes.MinRead
			}
			grown := make([]byte, len(b), newCap)
			copy(grown, b)
			b = grown
		}
		n, err := r.Read(b[len(b):cap(b)])
		b = b[:len(b)+n]
		if errors.Is(err, io.EOF) {
			return b, nil
		}
		if err != nil {
			return nil, err
		}
	}
}
