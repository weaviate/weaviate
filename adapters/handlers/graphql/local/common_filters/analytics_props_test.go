//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package common_filters

import (
	"fmt"
	"testing"

	"github.com/semi-technologies/weaviate/entities/filters"
	"github.com/semi-technologies/weaviate/usecases/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_ExtractAnalyticsProps(t *testing.T) {

	t.Run("when the analytics engine is turned off globally", func(t *testing.T) {
		cfg := config.AnalyticsEngine{
			Enabled:                   false,
			DefaultUseAnalyticsEngine: false,
		}
		args := map[string]interface{}{}
		expected := filters.AnalyticsProps{
			UseAnaltyicsEngine: false,
			ForceRecalculate:   false,
		}

		actual, err := ExtractAnalyticsProps(args, cfg)

		require.Nil(t, err)
		assert.Equal(t, expected, actual, "it should set 'use' to false")
	})

	t.Run("when the analytics engine is turned on and both fields set to false", func(t *testing.T) {
		cfg := config.AnalyticsEngine{
			Enabled:                   true,
			DefaultUseAnalyticsEngine: false,
		}
		args := map[string]interface{}{
			"useAnalyticsEngine": false,
			"forceRecalculate":   false,
		}
		expected := filters.AnalyticsProps{
			UseAnaltyicsEngine: false,
			ForceRecalculate:   false,
		}

		actual, err := ExtractAnalyticsProps(args, cfg)

		require.Nil(t, err)
		assert.Equal(t, expected, actual, "it should set 'use' to false")
	})

	t.Run("when the analytics engine is turned on and 'use' is true", func(t *testing.T) {
		cfg := config.AnalyticsEngine{
			Enabled:                   true,
			DefaultUseAnalyticsEngine: false,
		}
		args := map[string]interface{}{
			"useAnalyticsEngine": true,
			"forceRecalculate":   false,
		}
		expected := filters.AnalyticsProps{
			UseAnaltyicsEngine: true,
			ForceRecalculate:   false,
		}

		actual, err := ExtractAnalyticsProps(args, cfg)

		require.Nil(t, err)
		assert.Equal(t, expected, actual, "it should set 'use' to true")
	})

	t.Run("when the analytics engine is turned on and 'use' and 'force' are true", func(t *testing.T) {
		cfg := config.AnalyticsEngine{
			Enabled:                   true,
			DefaultUseAnalyticsEngine: false,
		}
		args := map[string]interface{}{
			"useAnalyticsEngine": true,
			"forceRecalculate":   true,
		}
		expected := filters.AnalyticsProps{
			UseAnaltyicsEngine: true,
			ForceRecalculate:   true,
		}

		actual, err := ExtractAnalyticsProps(args, cfg)

		require.Nil(t, err)
		assert.Equal(t, expected, actual, "it should set both to true")
	})

	t.Run("when 'force' is on although 'use' is off", func(t *testing.T) {
		cfg := config.AnalyticsEngine{
			Enabled:                   true,
			DefaultUseAnalyticsEngine: false,
		}
		args := map[string]interface{}{
			"useAnalyticsEngine": false,
			"forceRecalculate":   true,
		}

		_, err := ExtractAnalyticsProps(args, cfg)

		assert.Equal(t, fmt.Errorf("invalid arguments: 'forceRecalculate' cannot be set to true if "+
			"'useAnalyticsEngine' is set to false"), err)
	})

}
