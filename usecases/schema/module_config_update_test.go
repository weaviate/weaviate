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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUpdateClass_ModuleConfigNamesOnlyEnabledModules(t *testing.T) {
	const missing = "rerankr-openai"

	t.Run("rejects a module that does not exist", func(t *testing.T) {
		s := newSchemaWithModules(t)
		stored := s.create(t, namedVectorClass(mutableModule, moduleSettings("a", "m")))
		stored.ModuleConfig = map[string]any{generativeModule: map[string]any{"setting": "a"}}

		update := namedVectorClass(mutableModule, moduleSettings("a", "m"))
		update.ModuleConfig = map[string]any{
			generativeModule: map[string]any{"setting": "a"},
			missing:          map[string]any{"model": "x"},
		}
		require.EqualError(t, s.update(update), "module with name "+missing+" doesn't exist")
		require.Equal(t, map[string]any{generativeModule: map[string]any{"setting": "a"}}, stored.ModuleConfig)
	})

	t.Run("rejects a module that does not exist on a class without module config", func(t *testing.T) {
		s := newSchemaWithModules(t)
		s.create(t, namedVectorClass(mutableModule, moduleSettings("a", "m")))

		update := namedVectorClass(mutableModule, moduleSettings("a", "m"))
		update.ModuleConfig = map[string]any{missing: map[string]any{}}
		require.EqualError(t, s.update(update), "module with name "+missing+" doesn't exist")
	})

	t.Run("accepts a module that exists", func(t *testing.T) {
		s := newSchemaWithModules(t)
		stored := s.create(t, namedVectorClass(mutableModule, moduleSettings("a", "m")))

		update := namedVectorClass(mutableModule, moduleSettings("a", "m"))
		update.ModuleConfig = map[string]any{"reranker-fake": map[string]any{"model": "x"}}
		require.NoError(t, s.update(update))
		require.Equal(t, "x", storedModuleSettings(t, stored, "reranker-fake", "")["model"])
	})

	t.Run("keeps a stored module that is no longer enabled", func(t *testing.T) {
		// A module disabled after the class was created stays in its config;
		// an update that carries it unchanged must still pass.
		s := newSchemaWithModules(t)
		stored := s.create(t, namedVectorClass(mutableModule, moduleSettings("a", "m")))
		stored.ModuleConfig = map[string]any{missing: map[string]any{"model": "x"}}

		update := namedVectorClass(mutableModule, moduleSettings("a", "m"))
		update.ModuleConfig = map[string]any{missing: map[string]any{"model": "x"}}
		update.Description = "new description"
		require.NoError(t, s.update(update))
	})
}
