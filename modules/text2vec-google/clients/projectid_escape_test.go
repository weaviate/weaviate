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
	"context"
	"errors"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/usecases/modulecomponents/apikey"
)

type recordingTransport struct {
	got *url.URL
}

func (r *recordingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	r.got = req.URL
	return nil, errors.New("stop: request not sent")
}

func TestProjectIDCannotChangeTheRequestPath(t *testing.T) {
	for _, projectID := range []string{"my-project", "a?b=c", "a/../../other"} {
		t.Run(projectID, func(t *testing.T) {
			rt := &recordingTransport{}
			c := &google{
				apiKey:       "key",
				httpClient:   &http.Client{Transport: rt},
				googleApiKey: apikey.NewGoogleApiKey(),
				urlBuilderFn: buildURL,
				logger:       nullLogger(),
			}

			_, _ = c.vectorize(context.Background(), []string{"text"}, retrievalDocument, "", settings{
				ApiEndpoint: "us-central1-aiplatform.googleapis.com",
				ProjectID:   projectID,
				Model:       "gemini-embedding-001",
				Location:    "us-central1",
			})

			if rt.got == nil {
				// The project id was rejected before a request was built.
				return
			}
			want := "/v1/projects/" + url.PathEscape(projectID) +
				"/locations/us-central1/publishers/google/models/gemini-embedding-001:predict"
			assert.Equal(t, want, rt.got.EscapedPath())
			assert.Empty(t, rt.got.RawQuery)
		})
	}
}
