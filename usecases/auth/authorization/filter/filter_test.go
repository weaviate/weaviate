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

package filter

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
)

func TestFilter(t *testing.T) {
	tests := []struct {
		Name   string
		Config rbacconf.Config
		Items  []*models.Object
	}{
		{
			Name:   "rbac enabled, no objects",
			Items:  []*models.Object{},
			Config: rbacconf.Config{Enabled: true},
		},
		{
			Name:   "rbac disenabled, no objects",
			Items:  []*models.Object{},
			Config: rbacconf.Config{Enabled: false},
		},
	}

	authorizer := mocks.NewMockAuthorizer()
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			resourceFilter := New[*models.Object](authorizer, tt.Config)
			filteredObjects := resourceFilter.Filter(
				context.Background(),
				&models.Principal{Username: "user"},
				tt.Items,
				authorization.READ,
				func(obj *models.Object) string {
					return ""
				},
			)

			require.Equal(t, len(tt.Items), len(filteredObjects))
		})
	}
}

// ruleAuthorizer allows exactly the listed resource strings, mirroring an RBAC
// grant that covers ".../shards/*" but not the ".../shards/#" resource.
type ruleAuthorizer struct {
	allowed map[string]bool
}

func (a *ruleAuthorizer) Authorize(_ context.Context, _ *models.Principal, _ string, resources ...string) error {
	for _, resource := range resources {
		if !a.allowed[resource] {
			return errors.New("forbidden: " + resource)
		}
	}
	return nil
}

func (a *ruleAuthorizer) AuthorizeSilent(ctx context.Context, principal *models.Principal, verb string, resources ...string) error {
	return a.Authorize(ctx, principal, verb, resources...)
}

func (a *ruleAuthorizer) FilterAuthorizedResources(_ context.Context, _ *models.Principal, _ string, resources ...string) ([]string, error) {
	allowed := make([]string, 0, len(resources))
	for _, resource := range resources {
		if a.allowed[resource] {
			allowed = append(allowed, resource)
		}
	}
	return allowed, nil
}

// TestFilterSameParentShortcut pins when the single wildcard-parent check may
// stand in for per-item checks: never for a "#" (collection-itself) resource,
// whose wildcard form is the tenant permission shape ".../shards/*".
func TestFilterSameParentShortcut(t *testing.T) {
	tenantWildcardGrant := map[string]bool{
		authorization.ShardsMetadata("Cls")[0]: true, // schema/collections/Cls/shards/*
	}
	tests := []struct {
		name    string
		items   []string
		allowed map[string]bool
		want    []string
	}{
		{
			name:    "collection metadata '#' is not covered by a tenant wildcard grant",
			items:   authorization.CollectionsMetadata("Cls"),
			allowed: tenantWildcardGrant,
			want:    []string{},
		},
		{
			name:    "tenant items still use the wildcard-parent shortcut",
			items:   authorization.ShardsMetadata("Cls", "T1", "T2"),
			allowed: tenantWildcardGrant,
			want:    authorization.ShardsMetadata("Cls", "T1", "T2"),
		},
		{
			name:  "a '#' item anywhere in a mixed list disables the shortcut",
			items: append(authorization.ShardsMetadata("Cls", "T1"), authorization.CollectionsMetadata("Cls")...),
			allowed: map[string]bool{
				authorization.ShardsMetadata("Cls")[0]:       true, // schema/collections/Cls/shards/*
				authorization.ShardsMetadata("Cls", "T1")[0]: true,
			},
			want: authorization.ShardsMetadata("Cls", "T1"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := New[string](&ruleAuthorizer{allowed: tt.allowed}, rbacconf.Config{Enabled: true})
			got := f.Filter(
				context.Background(),
				&models.Principal{Username: "user"},
				tt.items,
				authorization.READ,
				func(resource string) string { return resource },
			)
			require.ElementsMatch(t, tt.want, got)
		})
	}
}
