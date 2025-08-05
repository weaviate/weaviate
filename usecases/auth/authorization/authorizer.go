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

package authorization

import (
	"context"

	"github.com/weaviate/weaviate/entities/models"
)

// Authorizer always makes a yes/no decision on a specific resource. Which
// authorization technique is used in the background (e.g. RBAC, adminlist,
// ...) is hidden through this interface
type Authorizer interface {
	Authorize(ctx context.Context, principal *models.Principal, verb string, resources ...string) error
	// AuthorizeSilent Silent authorization without audit logs
	AuthorizeSilent(ctx context.Context, principal *models.Principal, verb string, resources ...string) error
	// FilterAuthorizedResources authorize the passed resources with best effort approach, it will return
	// list of allowed resources, if none, it will return an empty slice
	FilterAuthorizedResources(ctx context.Context, principal *models.Principal, verb string, resources ...string) ([]string, error)
}

// DummyAuthorizer is a pluggable Authorizer which can be used if no specific
// authorizer is configured. It will allow every auth decision, i.e. it is
// effectively the same as "no authorization at all"
type DummyAuthorizer struct{}

// Authorize on the DummyAuthorizer will allow any subject access to any
// resource
func (d *DummyAuthorizer) Authorize(ctx context.Context, principal *models.Principal, verb string, resources ...string) error {
	return nil
}

func (d *DummyAuthorizer) AuthorizeSilent(ctx context.Context, principal *models.Principal, verb string, resources ...string) error {
	return nil
}

func (d *DummyAuthorizer) FilterAuthorizedResources(ctx context.Context, principal *models.Principal, verb string, resources ...string) ([]string, error) {
	return resources, nil
}
