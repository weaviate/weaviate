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

import "context"

type VectorizerClient interface {
	MultiVectorForWord(ctx context.Context,
		words []string) ([][]float32, error)
	VectorOnlyForCorpi(ctx context.Context, corpi []string,
		overrides map[string]string) ([]float32, error)
}

type MetaProvider interface {
	MetaInfo() (map[string]interface{}, error)
}

type Client interface {
	Vectorizers() map[string]VectorizerClient
}
