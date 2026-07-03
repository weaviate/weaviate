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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

type emptyTenantActivityStatusContextKey struct{}

type tenantCreateActivityStatus struct {
	Name           string  `json:"name"`
	ActivityStatus *string `json:"activityStatus"`
}

func markEmptyTenantActivityStatusOnCreate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !isTenantCreateRequest(r) {
			next.ServeHTTP(w, r)
			return
		}

		err, ok := emptyTenantActivityStatusError(r)
		if !ok {
			next.ServeHTTP(w, r)
			return
		}

		ctx := context.WithValue(r.Context(), emptyTenantActivityStatusContextKey{}, err)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func isTenantCreateRequest(r *http.Request) bool {
	if r.Method != http.MethodPost {
		return false
	}

	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	return len(parts) == 4 &&
		parts[0] == "v1" &&
		parts[1] == "schema" &&
		parts[2] != "" &&
		parts[3] == "tenants"
}

func emptyTenantActivityStatusError(r *http.Request) (error, bool) {
	if r.Body == nil {
		return nil, false
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, false
	}
	_ = r.Body.Close()
	r.Body = io.NopCloser(bytes.NewReader(body))

	var tenants []tenantCreateActivityStatus
	if err := json.Unmarshal(body, &tenants); err != nil {
		return nil, false
	}

	for _, tenant := range tenants {
		if tenant.ActivityStatus != nil && *tenant.ActivityStatus == "" {
			return fmt.Errorf("invalid activity status '' for tenant %q", tenant.Name), true
		}
	}

	return nil, false
}

func emptyTenantActivityStatusFromContext(ctx context.Context) (error, bool) {
	err, ok := ctx.Value(emptyTenantActivityStatusContextKey{}).(error)
	return err, ok
}
