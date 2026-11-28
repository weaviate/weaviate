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

package modulecomponents

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

func TestValidateAzureDeploymentID(t *testing.T) {
	tests := []struct {
		name  string
		value string
		valid bool
	}{
		{name: "letters", value: "d", valid: true},
		{name: "model name with periods and hyphens", value: "gpt-4.1-mini", valid: true},
		{name: "underscore", value: "my_deployment", valid: true},
		{name: "tilde", value: "team~1", valid: true},
		{name: "two periods inside a name", value: "..d", valid: true},
		{name: "empty", value: "", valid: true},
		{name: "current directory", value: "."},
		{name: "parent directory", value: ".."},
		{name: "parent directories and a path", value: "../../../team-b/x"},
		{name: "percent-encoded parent directory", value: "%2e%2e"},
		{name: "slash", value: "team-b/x"},
		{name: "backslash", value: `..\x`},
		{name: "query", value: "d?x"},
		{name: "fragment", value: "d#"},
		{name: "parent directory with a semicolon", value: "..;"},
		{name: "space", value: "d x"},
		{name: "userinfo", value: "x@y"},
		{name: "colon", value: "x:y"},
		{name: "trailing newline", value: "d\n"},
		{name: "non-ASCII letter", value: "dé"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateAzureDeploymentID(tt.value)
			if tt.valid {
				assert.NoError(t, err)
				return
			}
			assert.EqualError(t, err,
				`deploymentId must be letters, digits, hyphens, underscores, periods or tildes, and not "." or "..", got `+
					strconv.Quote(tt.value))
			var precondition enterrors.ErrModulePrecondition
			assert.ErrorAs(t, err, &precondition, "REST search returns the reason with a 422 only for this type")
		})
	}
}
