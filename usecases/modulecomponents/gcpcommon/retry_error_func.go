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

package gcpcommon

import (
	"strings"

	"cloud.google.com/go/storage"
	"github.com/pkg/errors"
	"google.golang.org/api/googleapi"
)

func RetryErrorFunc(err error) bool {
	if err == nil {
		return false
	}

	if storage.ShouldRetry(err) {
		return true
	}

	// Retry on http2 connection lost error which is not covered by ShouldRetry
	if strings.Contains(err.Error(), "http2: client connection lost") {
		return true
	}

	var gerr *googleapi.Error
	if errors.As(err, &gerr) {
		// retry on 401 on top of the default retryable errors
		return gerr.Code == 401
	}

	return false
}
