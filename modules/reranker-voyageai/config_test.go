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

package modrerankervoyageai

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/usecases/modulecomponents/rerankertest"
)

// TestValidateClass proves the module actually wires class-setting
// validation into ValidateClass (the class-creation/update path), not just
// the classSettings.Validate method tested in isolation by
// config/class_settings_test.go - see the Copilot review on PR #12984 for
// context: this used to unconditionally return nil.
func TestValidateClass(t *testing.T) {
	t.Setenv("MODULES_VALIDATE_BASE_URL", "true")
	m := New()

	t.Run("rejects a non-HTTPS baseURL", func(t *testing.T) {
		err := m.ValidateClass(context.Background(), nil, rerankertest.FakeClassConfig{
			ClassConfig: map[string]interface{}{"baseURL": "http://api.example.com"},
		})
		assert.Error(t, err)
	})

	t.Run("accepts the default settings", func(t *testing.T) {
		err := m.ValidateClass(context.Background(), nil, rerankertest.FakeClassConfig{
			ClassConfig: map[string]interface{}{},
		})
		assert.NoError(t, err)
	})
}
