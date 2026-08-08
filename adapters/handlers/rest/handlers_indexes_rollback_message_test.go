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

package rest

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/backup"
)

// The submit is refused by whichever of the two operations claimed the slot,
// and the advice it gives ("retry after it finishes") is about that one. Naming
// a backup while a restore holds the slot sends the operator to watch the wrong
// thing; the sibling backupBusyResponder already names the kind it saw.
func TestRollbackFailedRefusalNamesTheOperationThatBlocked(t *testing.T) {
	tests := []struct {
		name     string
		activity backup.NodeActivity
		want     string
	}{
		{
			name:     "a backup holds the slot",
			activity: backup.NodeActivity{Busy: true, Kind: backup.NodeActivityKindBackup},
			want:     "a backup is running",
		},
		{
			name:     "a restore holds the slot",
			activity: backup.NodeActivity{Busy: true, Kind: backup.NodeActivityKindRestore},
			want:     "a restore is running",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			responder := reindexTaskRollbackFailedResponder(&models.Principal{Username: "u1"},
				tc.activity, "Movies:change_tokenization:title:ab3f")

			conflict, ok := responder.(*schema.SchemaObjectsIndexesUpdateConflict)
			require.Truef(t, ok, "the rollback failure is a 409, got %T", responder)

			msg := conflict.Payload.Error[0].Message
			require.Contains(t, msg, tc.want)
			require.Contains(t, msg, "retry after the "+tc.activity.Kind+" finishes")
			require.Contains(t, msg, "Movies:change_tokenization:title:ab3f",
				"the id is the only handle the caller has on the migration that kept running")
		})
	}
}
