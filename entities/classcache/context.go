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
	"slices"

	"github.com/weaviate/weaviate/entities/versioned"
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

func ClassesFromContext(ctxWithClassCache context.Context, getter func(names ...string) (map[string]versioned.Class, error), names ...string) (map[string]versioned.Class, error) {
	cache, err := extractCache(ctxWithClassCache)
	if err != nil {
		return nil, err
	}

	versionedClasses := map[string]versioned.Class{}
	notFoundInCtx := []string{}
	for _, name := range names {
		// collect what is not in context
		if entry, ok := cache.Load(name); ok {
			versionedClasses[entry.class.Class] = versioned.Class{Class: entry.class, Version: entry.version}
			continue
		}
		notFoundInCtx = append(notFoundInCtx, name)
	}

	// remove dedup, empty and a void calls if there is non
	slices.Sort(notFoundInCtx)
	notFoundInCtx = slices.Compact(notFoundInCtx)
	if len(notFoundInCtx) == 0 {
		return versionedClasses, nil
	}

	if len(notFoundInCtx) > 1 && notFoundInCtx[0] == "" {
		notFoundInCtx = notFoundInCtx[1:]
	}

	// TODO prevent concurrent getter calls for the same class if it was not loaded,
	// get once and share results
	vclasses, err := getter(notFoundInCtx...)
	if err != nil {
		return versionedClasses, err
	}

	for _, vclass := range vclasses {
		// do not replace entry if it was loaded in the meantime by concurrent access
		entry, _ := cache.LoadOrStore(vclass.Class.Class, &classCacheEntry{class: vclass.Class, version: vclass.Version})
		versionedClasses[entry.class.Class] = versioned.Class{Class: entry.class, Version: entry.version}
	}

	return versionedClasses, nil
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
