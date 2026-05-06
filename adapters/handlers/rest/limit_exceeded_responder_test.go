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
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/weaviate/weaviate/usecases/usagelimits"
)

func TestLimitExceededResponder_WritesStructured429(t *testing.T) {
	err := &usagelimits.LimitExceededError{
		Limit:           usagelimits.LimitObjects,
		Value:           10000,
		RenderedMessage: "hit limit of 10000 objects, upgrade pls",
	}
	r := newLimitExceededResponder(err)
	rec := httptest.NewRecorder()
	r.WriteResponse(rec, nil)

	if rec.Code != 429 {
		t.Fatalf("status = %d, want 429", rec.Code)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", got)
	}
	var body usageLimitErrorBody
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("body is not JSON: %v\n%s", err, rec.Body.String())
	}
	if body.ErrorCode != usagelimits.ErrorCode {
		t.Errorf("errorCode = %q, want %q", body.ErrorCode, usagelimits.ErrorCode)
	}
	if body.Limit != string(usagelimits.LimitObjects) {
		t.Errorf("limit = %q, want %q", body.Limit, usagelimits.LimitObjects)
	}
	if body.Value != 10000 {
		t.Errorf("value = %d, want 10000", body.Value)
	}
	if body.Message != "hit limit of 10000 objects, upgrade pls" {
		t.Errorf("message = %q, want substituted template", body.Message)
	}
}
