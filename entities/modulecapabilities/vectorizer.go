package modulecapabilities

import (
	"context"

	"github.com/semi-technologies/weaviate/entities/models"
)

type Vectorizer interface {
	// UpdateObject should mutate the object which is passed in as a pointer-type
	// by extending it with the desired vector and - if applicable - any meta
	// information as part of _additional properties
	UpdateObject(ctx context.Context, obj *models.Object, cfg ClassConfig) error
}
