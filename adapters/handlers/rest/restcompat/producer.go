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

package restcompat

import (
	"io"

	"github.com/go-openapi/runtime"

	"github.com/weaviate/weaviate/entities/models"
)

// NewJSONProducer wraps the default JSON producer to inject the legacy
// asyncEnabled field on *models.Class and *models.Schema payloads. Other
// payloads pass through unchanged.
func NewJSONProducer() runtime.Producer {
	base := runtime.JSONProducer()
	return runtime.ProducerFunc(func(w io.Writer, v interface{}) error {
		switch t := v.(type) {
		case *models.Class:
			return base.Produce(w, wrapClass(t))
		case *models.Schema:
			return base.Produce(w, wrapSchema(t))
		default:
			return base.Produce(w, v)
		}
	})
}
