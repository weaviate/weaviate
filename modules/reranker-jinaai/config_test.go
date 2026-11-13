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

package modrerankerjinaai

import (
	"testing"

	"github.com/weaviate/weaviate/usecases/modulecomponents/rerankertest"
)

// TestValidateClass proves the module actually wires class-setting
// validation into ValidateClass (the class-creation/update path), not just
// the classSettings.Validate method tested in isolation by
// config/class_settings_test.go - see the Copilot review on PR #12984 for
// context: this used to unconditionally return nil.
func TestValidateClass(t *testing.T) {
	rerankertest.RunValidateClassTest(t, New())
}
