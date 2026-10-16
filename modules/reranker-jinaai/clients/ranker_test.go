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
	"encoding/json"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/weaviate/weaviate/usecases/modulecomponents/rerankertest"
)

func nullLogger() logrus.FieldLogger {
	l, _ := test.NewNullLogger()
	return l
}

// buildResponse and buildError encode Jinaai's own wire format (a "results"
// array, and a {"detail": ...} error envelope); everything else about
// TestRank is shared via rerankertest.RunRankTest.
func buildResponse(results []Result) ([]byte, error) {
	return json.Marshal(RankResponse{Results: results})
}

func buildError(message string) []byte {
	return []byte(`{"detail":"` + message + `"}`)
}

func TestRank(t *testing.T) {
	batchClient := New("apiKey", 0, nullLogger())
	batchClient.maxDocuments = 2 // this will trigger 4 go routines

	rerankertest.RunRankTest[Result](t,
		New("apiKey", 0, nullLogger()),
		batchClient,
		func(index int, score float64) Result {
			return Result{Index: index, RelevanceScore: score}
		},
		buildResponse, buildError)
}

func TestRank_client_getJinaaiUrl(t *testing.T) {
	c := New("", 1*time.Second, nil)
	rerankertest.AssertBaseURLOverride(t, c.getJinaaiUrl,
		"https://api.jina.ai", "https://api.jina.ai/v1/rerank",
		"X-Jinaai-Baseurl", "https://base-url-from-ctx.com", "https://base-url-from-ctx.com/v1/rerank")
}
