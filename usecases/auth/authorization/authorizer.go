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

package authorization

import (
	"context"

	"github.com/weaviate/weaviate/entities/models"
)

// Authorizer always makes a yes/no decision on a specific resource. Which
// authorization technique is used in the background (e.g. RBAC, adminlist,
// ...) is hidden through this interface, except that only RBAC reads
// namespace state.
type Authorizer interface {
	Authorize(ctx context.Context, principal *models.Principal, verb string, resources ...string) error
	// AuthorizeAndRequireActiveNamespace runs Authorize first, so a caller
	// denied the resource never learns the namespace's state, then requires
	// class's namespace to be active. Pass the resolved qualified name the
	// resources were built from. A namespace refusal is RequireActive's
	// sentinel, unwrapped and never a Forbidden — RequireActive rather than
	// AdmitDestructiveApply, so a deleting namespace refuses here. A caller past
	// Authorize is therefore told the namespace's state, including that no such
	// namespace exists, and reads the sentinel's own text rather than
	// namespaces.PublicMessage's. A confined caller is told only its own
	// namespace's state, because namespacing.Resolve rejects a qualified
	// class name from it before these resources are built.
	AuthorizeAndRequireActiveNamespace(ctx context.Context, principal *models.Principal, verb string, class string, resources ...string) error
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

// AuthorizeAndRequireActiveNamespace skips the namespace check: this package
// sits inside usecases/namespaces' import closure and cannot import it. Safe
// only while Config.Validate requires RBAC whenever NAMESPACES_ENABLED is set;
// once a single collection can be suspended without that requirement, this
// must refuse instead.
func (d *DummyAuthorizer) AuthorizeAndRequireActiveNamespace(ctx context.Context, principal *models.Principal, verb string, class string, resources ...string) error {
	return nil
}

func (d *DummyAuthorizer) AuthorizeSilent(ctx context.Context, principal *models.Principal, verb string, resources ...string) error {
	return nil
}

func (d *DummyAuthorizer) FilterAuthorizedResources(ctx context.Context, principal *models.Principal, verb string, resources ...string) ([]string, error) {
	return resources, nil
}
