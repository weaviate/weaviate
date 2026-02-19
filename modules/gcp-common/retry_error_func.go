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
