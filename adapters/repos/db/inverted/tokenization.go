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

package inverted

import "github.com/weaviate/weaviate/adapters/repos/db/lsmkv"

// TokenizationBucketPinner returns a property's tokenization and pins its bucket as one snapshot,
// until release runs exactly once (bucket may be nil): a retokenization's flip moves both.
type TokenizationBucketPinner func(propName, bucketName, schemaTokenization string) (tokenization string, bucket *lsmkv.Bucket, release func())
