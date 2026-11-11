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

package objects

import (
	"context"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/search"
)

// tenantFinder records the tenant ObjectByID receives, which fakeObjectFinder drops.
type tenantFinder struct {
	*fakeObjectFinder
	tenant string
}

func (f *tenantFinder) ObjectByID(ctx context.Context, id strfmt.UUID, props search.SelectProperties,
	addl additional.Properties, tenant string,
) (*search.Result, error) {
	f.tenant = tenant
	return f.fakeObjectFinder.ObjectByID(ctx, id, props, addl, tenant)
}

func Test_findObjectFor(t *testing.T) {
	const tenant = "tenantA"
	id := strfmt.UUID("5b6a08ba-1d46-43aa-89cc-8b070790c6f2")
	stored := &search.Result{ID: id}

	tests := []struct {
		name              string
		namespacesEnabled bool
		sourceClass       string
		lookupClass       string
		// readClass is the class repo.Object reads. Empty means a read by id.
		readClass string
	}{
		{
			name:              "beacon's short class reads in the source's namespace",
			namespacesEnabled: true,
			sourceClass:       "customer1:Zoo",
			lookupClass:       "Animal",
			readClass:         "customer1:Animal",
		},
		{
			name:        "classless beacon reads by id",
			sourceClass: "Zoo",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			repo := &tenantFinder{fakeObjectFinder: &fakeObjectFinder{}}
			if tc.readClass == "" {
				repo.On("ObjectByID", id, search.SelectProperties(nil), additional.Properties{}).
					Return(stored, nil).Once()
			} else {
				repo.On("Object", tc.readClass, id, search.SelectProperties(nil), additional.Properties{}, tenant).
					Return(stored, nil).Once()
			}

			lookup := findObjectFor(repo, qualifierFor(tc.namespacesEnabled), tc.sourceClass)
			res, err := lookup(context.Background(), tc.lookupClass, id, nil, additional.Properties{}, tenant)

			require.NoError(t, err)
			assert.Same(t, stored, res)
			if tc.readClass == "" {
				assert.Equal(t, tenant, repo.tenant)
			}
			repo.AssertExpectations(t)
		})
	}
}
