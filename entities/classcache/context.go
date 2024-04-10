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

package classcache

import (
	"context"
	"fmt"

	"github.com/weaviate/weaviate/entities/models"
)

const classCacheKey = "classCache"

var errorNoClassCache = fmt.Errorf("context does not contain classCache")

func ContextWithClassCache(ctx context.Context) context.Context {
	if ctx.Value(classCacheKey) != nil {
		return ctx
	}
	return context.WithValue(ctx, classCacheKey, &classCache{})
}

func ClassFromContext(ctxWithClassCache context.Context, name string, getter func(name string) (*models.Class, error)) (*models.Class, error) {
	value := ctxWithClassCache.Value(classCacheKey)
	if value == nil {
		return nil, errorNoClassCache
	}
	cache, ok := value.(*classCache)
	if !ok {
		return nil, errorNoClassCache
	}

	if class, ok := cache.Load(name); ok {
		return class, nil
	}

	class, err := getter(name)
	if err != nil {
		return nil, err
	}
	cache.Store(name, class)
	return class, nil
}
