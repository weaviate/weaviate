//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package errors

import "testing"

func TestReplicationErrorStatus(t *testing.T) {
	tests := []struct {
		code StatusCode
		desc string
	}{
		{-1, ""},
		{_StatusCodeOK, "ok"},
		{_StatusClassNotFound, "class not found"},
		{_StatusShardNotFound, "shard not found"},
		{_StatusNotFound, "not found"},
		{_StatusAlreadyExisted, "already existed"},
		{_StatusConflict, "conflict"},
		{_StatusPreconditionFailed, "precondition failed"},
		{_StatusReadOnly, "read only"},
	}
	for _, test := range tests {
		got := StatusText(test.code)
		if got != test.desc {
			t.Errorf("StatusText(%d) want %v got %v", test.code, got, test.desc)
		}
	}
}
