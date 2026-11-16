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

// TokenizationBucketPinner returns one consistent (tokenization, bucket)
// snapshot of a property, pinned until release runs exactly once (bucket may
// be nil; release is still required). A retokenization's flip moves both, and
// a query that read one from before it and one from after would find nothing.
type TokenizationBucketPinner func(propName, bucketName, schemaTokenization string) (tokenization string, bucket *lsmkv.Bucket, release func())
