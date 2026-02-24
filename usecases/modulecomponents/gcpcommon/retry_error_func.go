package gcpcommon

import (
	"cloud.google.com/go/storage"
	"github.com/pkg/errors"
	"google.golang.org/api/googleapi"
)

func RetryErrorFunc(err error) bool {
	if storage.ShouldRetry(err) {
		return true
	}

	var gerr *googleapi.Error
	if errors.As(err, &gerr) {
		// retry on 401 on top of the default retryable errors
		return gerr.Code == 401
	}

	return false
}
