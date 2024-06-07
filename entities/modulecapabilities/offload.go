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

package modulecapabilities

import (
	"context"
)

type OffloadCloud interface {
	Upload(ctx context.Context, className, shardName string) error
	Download(ctx context.Context, className, shardName string) error
}

type OffloadProvider interface {
	OffloadBackend(backend string) (OffloadCloud, error)
}
