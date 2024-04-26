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

func RemoveClassFromContext(ctxWithClassCache context.Context, name string) error {
	cache, err := extractCache(ctxWithClassCache)
	if err != nil {
		return err
	}

	cache.Delete(name)
	return nil
}

func ClassFromContext(ctxWithClassCache context.Context, name string, getter func(name string) (*models.Class, uint64, error)) (*models.Class, uint64, error) {
	cache, err := extractCache(ctxWithClassCache)
	if err != nil {
		return nil, 0, err
	}

	if entry, ok := cache.Load(name); ok {
		return entry.class, entry.version, nil
	}

	// TODO prevent concurrent getter calls for the same class if it was not loaded,
	// get once and share results
	class, version, err := getter(name)
	if err != nil {
		return nil, 0, err
	}

	// do not replace entry if it was loaded in the meantime by concurrent access
	entry, _ := cache.LoadOrStore(name, &classCacheEntry{class: class, version: version})
	return entry.class, entry.version, nil
}

func extractCache(ctx context.Context) (*classCache, error) {
	value := ctx.Value(classCacheKey)
	if value == nil {
		return nil, errorNoClassCache
	}
	cache, ok := value.(*classCache)
	if !ok {
		return nil, errorNoClassCache
	}
	return cache, nil
}
