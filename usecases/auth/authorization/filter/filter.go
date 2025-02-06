package filter

import (
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/config"
)

// ResourceFilter handles filtering resources based on authorization
type ResourceFilter[T any] struct {
	authorizer authorization.Authorizer
	config     config.Config
}

func New[T any](authorizer authorization.Authorizer, config config.Config) *ResourceFilter[T] {
	return &ResourceFilter[T]{
		authorizer: authorizer,
		config:     config,
	}
}

// FilterFn defines a function that generates authorization resources for an item
type FilterFn[T any] func(item T) []string

// Filter filters a slice of items based on authorization
func (f *ResourceFilter[T]) Filter(
	principal *models.Principal,
	items []T,
	verb string,
	resourceFn FilterFn[T],
) []T {
	if !f.config.Authorization.Rbac.Enabled {
		if len(items) == 0 {
			return items
		}
		// here it's either you have the permissions or not so 1 check is enough
		if err := f.authorizer.Authorize(principal, verb, resourceFn(items[0])...); err != nil {
			return nil
		}
		return items
	}

	// For RBAC, filter based on per-item authorization
	filtered := make([]T, 0, len(items))
	for _, item := range items {
		// TODO-RBAC: we need non silent authorizer with best effort enforcer
		if err := f.authorizer.Authorize(principal, verb, resourceFn(item)...); err == nil {
			filtered = append(filtered, item)
		}
	}
	return filtered
}
