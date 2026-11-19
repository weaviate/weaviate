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

package schema

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
)

// restoreClassWithRealParser runs RestoreClass on class through the
// production vector config parser and returns the error together with
// whether the class reached the schema manager.
func restoreClassWithRealParser(t *testing.T, class *models.Class) (error, bool) {
	t.Helper()
	handler, schemaManager := newTestHandlerAndManagerWithRealVectorConfigParser(t)
	handler.asyncIndexingEnabled = true

	schemaManager.On("ReadOnlyClass", mock.Anything).Return((*models.Class)(nil)).Maybe()
	restored := false
	schemaManager.On("RestoreClass", mock.Anything, mock.Anything).
		Run(func(mock.Arguments) { restored = true }).
		Return(nil).Maybe()

	schemaBytes, err := json.Marshal(class)
	require.NoError(t, err)
	descriptor := backup.ClassDescriptor{Name: class.Class, Schema: schemaBytes}
	err = handler.RestoreClass(context.Background(), &descriptor, map[string]string{}, false, false)
	return err, restored
}

// TestRestoreClassSkipsCreateUpdateOnlyChecks pins the review finding on
// #13656: backup restore must not apply checks that only govern schema
// writes. A class with RQ on hamming or manhattan was accepted before RQ
// distances were validated, and one with out-of-range muvera parameters
// before those bounds existed; both are persisted state and must restore,
// as they load at startup. The create path still rejects both.
func TestRestoreClassSkipsCreateUpdateOnlyChecks(t *testing.T) {
	for _, distance := range []string{"hamming", "manhattan"} {
		for _, shape := range rqClassShapes {
			for _, target := range rqTargets {
				name := distance + "/" + shape.name + "/" + rqTargetName(target.indexType, target.arm, target.bits, target.centering)
				t.Run(name, func(t *testing.T) {
					build := func() *models.Class {
						return shape.class(target.indexType, rqIndexConfig(target.indexType, target.arm, distance,
							rqMap(target.bits, target.centering)))
					}

					err, restored := restoreClassWithRealParser(t, build())
					require.NoError(t, err)
					require.True(t, restored)

					handler, _ := newTestHandlerAndManagerWithRealVectorConfigParser(t)
					handler.asyncIndexingEnabled = true
					require.ErrorContains(t, handler.validateVectorSettingsAgainst(build(), nil), "RQ")
				})
			}
		}
	}

	t.Run("muvera bounds", func(t *testing.T) {
		build := func() *models.Class { return classWithMuveraParams(1_000_000, 0, 0) }

		err, restored := restoreClassWithRealParser(t, build())
		require.NoError(t, err)
		require.True(t, restored)

		handler := newTestHandlerWithRealVectorConfigParser(t)
		require.ErrorContains(t, handler.validateVectorSettingsAgainst(build(), nil), "ksim")
	})
}
