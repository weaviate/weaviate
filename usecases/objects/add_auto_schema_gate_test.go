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
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization/mocks"
)

// Test_AddObject_GateRunsBeforeAutoSchema pins that the authorizer call at
// add.go:45 runs before auto-schema proposes a property, so a refused write
// never reaches AddClassProperty. The two errors differ so the one that comes
// back names which layer refused.
func Test_AddObject_GateRunsBeforeAutoSchema(t *testing.T) {
	gateRefused := errors.New("gate refused the write")
	proposeReached := errors.New("auto-schema proposed a property")

	// An object carrying a property the class does not declare, which is what
	// makes auto-schema propose one.
	newObject := func() *models.Object {
		return &models.Object{
			Class:      "Zoo",
			Properties: map[string]any{"undeclared": "value"},
		}
	}

	t.Run("an allowed write reaches the propose", func(t *testing.T) {
		m, _, _, _, _ := newNSManagers(t, zooAnimalNSSchema(false), false, withAutoSchema(proposeReached))

		_, err := m.AddObject(context.Background(), &models.Principal{Username: "u"}, newObject(), nil)

		require.ErrorIs(t, err, proposeReached,
			"without this fixture reaching auto-schema, the refusal row below proves nothing")
	})

	t.Run("a refused write stops at the gate", func(t *testing.T) {
		m, _, _, _, authz := newNSManagers(t, zooAnimalNSSchema(false), false, withAutoSchema(proposeReached))
		authz.SetErr(gateRefused)

		_, err := m.AddObject(context.Background(), &models.Principal{Username: "u"}, newObject(), nil)

		require.ErrorIs(t, err, gateRefused)
		require.NotErrorIs(t, err, proposeReached,
			"the write must be refused before any property is proposed")
		require.Len(t, authz.Calls(), 1)
		require.Equal(t, mocks.MethodAuthorizeAndRequireActiveNamespace, authz.Calls()[0].Method,
			"the refusal must come from the namespace gate rather than plain authorization")
		require.Equal(t, "Zoo", authz.Calls()[0].Class,
			"the gate must receive the resolved class")
	})
}
