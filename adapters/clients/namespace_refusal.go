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

package clients

import (
	"net/http"
	"strings"

	"github.com/weaviate/weaviate/usecases/namespaces"
	"github.com/weaviate/weaviate/usecases/replica"
)

// refusalError is err with a namespace refusal added to its chain. Error keeps
// err's text, and errors.As still finds err.
type refusalError struct {
	err      error
	sentinel error
}

func (e *refusalError) Error() string { return e.err.Error() }

func (e *refusalError) Unwrap() []error { return []error{e.err, e.sentinel} }

// rebuildRefusal returns err with the namespace refusal RefusalFromText finds
// in text, or err itself when it finds none.
func rebuildRefusal(err error, text string) error {
	sentinel := namespaces.RefusalFromText(text)
	if sentinel == nil {
		return err
	}
	return &refusalError{err: err, sentinel: sentinel}
}

// rebuildHTTPRefusal calls rebuildRefusal on the body of a 422 or a 503, the
// statuses an owning node answers a namespace refusal with.
func rebuildHTTPRefusal(err error, code int, body []byte) error {
	if code != http.StatusUnprocessableEntity && code != http.StatusServiceUnavailable {
		return err
	}
	return rebuildRefusal(err, string(body))
}

// rebuildObjectRefusal calls rebuildRefusal on a per-object error decoded from
// a 200 body, which carries no status to gate on.
func rebuildObjectRefusal(err error) error {
	if err == nil {
		return nil
	}
	return rebuildRefusal(err, err.Error())
}

// rebuildResponseRefusals sets Err to the namespace refusal each entry's Msg
// ends with. It trims that text and the ": " before it from Msg, so the entry's
// Error prints the refusal once.
func rebuildResponseRefusals(resp *replica.SimpleResponse) {
	for i := range resp.Errors {
		e := &resp.Errors[i]
		sentinel := namespaces.RefusalFromText(e.Msg)
		if sentinel == nil || e.Err != nil {
			continue
		}
		msg := strings.TrimSuffix(strings.TrimSpace(e.Msg), sentinel.Error())
		e.Msg, e.Err = strings.TrimSuffix(msg, ": "), sentinel
	}
}
