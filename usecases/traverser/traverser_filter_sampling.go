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

package traverser

import (
	"context"
	"fmt"

	"github.com/weaviate/weaviate/entities/filtersampling"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

// FilterSampling samples unique values from a property's inverted index
func (t *Traverser) FilterSampling(ctx context.Context, principal *models.Principal,
	params *filtersampling.Params,
) (*filtersampling.Result, error) {
	// Resolve alias if needed
	if cls := t.schemaGetter.ResolveAlias(params.ClassName.String()); cls != "" {
		params.ClassName = schema.ClassName(cls)
	}

	// Validate class exists
	class := t.schemaGetter.ReadOnlyClass(params.ClassName.String())
	if class == nil {
		return nil, fmt.Errorf("could not find class %s in schema", params.ClassName)
	}

	// Delegate to vectorSearcher
	return t.vectorSearcher.FilterSampling(ctx, *params)
}
