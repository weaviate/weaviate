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

package inverted

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A failed range-index migration leaves the index withheld just as a cancelled
// one does, and nothing runs afterwards that would end the wait.
func TestAWithheldRangeIndexNamesTheReloadAfterAFailedMigration(t *testing.T) {
	require.Contains(t, NewWithheldRangeableIndexError("price").Error(),
		"if it was cancelled or failed, reload the shard")
}
