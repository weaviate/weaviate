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

// buildResponse and buildError encode VoyageAI's own wire format (a "data"
// array, and a {"message": ...} error envelope); everything else about
// TestRank is shared via rerankertest.RunRankTest.
func buildResponse(results []Data) ([]byte, error) {
	return json.Marshal(RankResponse{Data: results})
}

func buildError(message string) []byte {
	return []byte(`{"message":"` + message + `"}`)
}

func TestRank(t *testing.T) {
	batchClient := New("apiKey", 0, nullLogger())
	batchClient.maxDocuments = 2 // this will trigger 4 go routines

	rerankertest.RunRankTest[Data](t,
		New("apiKey", 0, nullLogger()),
		batchClient,
		func(index int, score float64) Data {
			return Data{Index: index, RelevanceScore: score}
		},
		buildResponse, buildError)
}

func TestRank_client_getVoyageAIUrl(t *testing.T) {
	c := New("", 1*time.Second, nil)
	rerankertest.AssertBaseURLOverride(t, c.getVoyageAIUrl,
		"https://api.voyageai.com/v1", "https://api.voyageai.com/v1/rerank",
		"X-Voyageai-Baseurl", "https://base-url-from-ctx.com", "https://base-url-from-ctx.com/rerank")
}
