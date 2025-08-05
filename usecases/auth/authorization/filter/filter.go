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

package filter

import (
	"context"
	"slices"

	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"

	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

// ResourceFilter handles filtering resources based on authorization
type ResourceFilter[T any] struct {
	authorizer authorization.Authorizer
	config     rbacconf.Config
}

func New[T any](authorizer authorization.Authorizer, config rbacconf.Config) *ResourceFilter[T] {
	return &ResourceFilter[T]{
		authorizer: authorizer,
		config:     config,
	}
}

// FilterFn defines a function that generates authorization resources for an item
type FilterFn[T any] func(item T) string

// Filter filters a slice of items based on authorization
func (f *ResourceFilter[T]) Filter(
	ctx context.Context,
	logger logrus.FieldLogger,
	principal *models.Principal,
	items []T,
	verb string,
	resourceFn FilterFn[T],
) []T {
	if len(items) == 0 {
		return items
	}
	if !f.config.Enabled {
		// here it's either you have the permissions or not so 1 check is enough
		if err := f.authorizer.Authorize(ctx, principal, verb, resourceFn(items[0])); err != nil {
			logger.WithFields(logrus.Fields{
				"username":  principal.Username,
				"verb":      verb,
				"resources": items,
			}).Error(err)
			return nil
		}
		return items
	}

	// For RBAC, first check if all items have the same parent resource
	firstResource := resourceFn(items[0])
	allSameParent := true

	for i := 1; i < len(items); i++ {
		if authorization.WildcardPath(resourceFn(items[i])) != authorization.WildcardPath(firstResource) {
			allSameParent = false
		}
	}

	// If all items have the same parent, we can do a single authorization check
	if allSameParent {
		err := f.authorizer.Authorize(ctx, principal, verb, authorization.WildcardPath(firstResource))
		if err != nil {
			logger.WithFields(logrus.Fields{
				"username": principal.Username,
				"verb":     verb,
				"resource": authorization.WildcardPath(firstResource),
			}).Error(err)
		}

		if err == nil {
			// user is authorized
			return items
		}
	}

	// For RBAC, filter based on per-item authorization
	filtered := make([]T, 0, len(items))
	resources := make([]string, 0, len(items))
	for _, item := range items {
		resources = append(resources, resourceFn(item))
	}

	allowedList, err := f.authorizer.FilterAuthorizedResources(ctx, principal, verb, resources...)
	if err != nil {
		logger.WithFields(logrus.Fields{
			"username":  principal.Username,
			"verb":      verb,
			"resources": resources,
		}).Error(err)
	}

	if len(allowedList) == len(resources) {
		// has permissions to all
		return items
	}

	for _, item := range items {
		if slices.Contains(allowedList, resourceFn(item)) {
			filtered = append(filtered, item)
		}
	}

	return filtered
}
