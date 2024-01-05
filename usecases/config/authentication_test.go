//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package config

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfig_Authentication(t *testing.T) {
	t.Run("no auth selected", func(t *testing.T) {
		auth := Authentication{}
		expected := fmt.Errorf("no authentication scheme configured, you must select at least one")

		err := auth.Validate()

		assert.Equal(t, expected, err)
	})

	t.Run("only anonymous selected", func(t *testing.T) {
		auth := Authentication{
			AnonymousAccess: AnonymousAccess{
				Enabled: true,
			},
		}

		err := auth.Validate()

		assert.Nil(t, err, "should not error")
	})

	t.Run("only oidc selected", func(t *testing.T) {
		auth := Authentication{
			OIDC: OIDC{
				Enabled: true,
			},
		}

		err := auth.Validate()

		assert.Nil(t, err, "should not error")
	})

	t.Run("oidc and anonymous enabled together", func(t *testing.T) {
		// this might seem counter-intuitive at first, but this makes a lot of
		// sense when you consider the authorization strategies: for example we
		// could allow reads for everyone, but only explicitly authenticated users
		// may write
		auth := Authentication{
			OIDC: OIDC{
				Enabled: true,
			},
			AnonymousAccess: AnonymousAccess{
				Enabled: true,
			},
		}

		err := auth.Validate()

		assert.Nil(t, err, "should not error")
	})
}
