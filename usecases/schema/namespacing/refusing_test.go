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

package namespacing

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	autherrs "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/license"
)

func TestRefusing(t *testing.T) {
	q := Refusing(license.Required("namespaces"))
	qualify := func(p *models.Principal) func() error {
		return func() error { _, err := q.Qualify(p, "Movies"); return err }
	}
	qualifyForCreate := func(p *models.Principal) func() error {
		return func() error { _, err := q.QualifyForCreate(p, "Movies"); return err }
	}

	require.True(t, q.NamespacesEnabled())

	cases := []struct {
		name string
		call func() error
	}{
		{name: "Qualify, global operator", call: qualify(globalPrincipal)},
		{name: "Qualify, namespaced principal", call: qualify(namespacedPrincipal)},
		{name: "Qualify, nil principal", call: qualify(nil)},
		{name: "QualifyForCreate, global operator", call: qualifyForCreate(globalPrincipal)},
		{name: "QualifyForCreate, namespaced principal", call: qualifyForCreate(namespacedPrincipal)},
		{name: "QualifyForCreate, nil principal", call: qualifyForCreate(nil)},
		{name: "QualifyRefTarget", call: func() error {
			_, _, err := q.QualifyRefTarget("customer1:Movies", "Actors")
			return err
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call()
			require.ErrorIs(t, err, license.ErrRequired)
			var forbidden autherrs.Forbidden
			require.ErrorAs(t, err, &forbidden)
		})
	}
}
