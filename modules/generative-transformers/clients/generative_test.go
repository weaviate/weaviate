//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

// //                           _       _
// // __      _____  __ ___   ___  __ _| |_ ___
// // \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
// //  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
// //   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
// //
// //  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
// //
// //  CONTACT: hello@weaviate.io
// //

package clients

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	generativemodels "github.com/weaviate/weaviate/usecases/modulecomponents/additional/models"
)

func nullLogger() logrus.FieldLogger {
	l, _ := test.NewNullLogger()
	return l
}

func TestGetAnswer(t *testing.T) {
	textProperties := []map[string]string{{"prop": "Say Test result"}}
	t.Run("when the server has a successful answer ", func(t *testing.T) {
		handler := &testAnswerHandler{
			t: t,
			answer: generateResponse{
				Result: "Test result",
			},
		}
		server := httptest.NewServer(handler)
		defer server.Close()

		c := New(server.URL, nullLogger())

		expected := generativemodels.GenerateResponse{
			Result: ptString("Test result"),
		}

		res, err := c.GenerateAllResults(context.Background(), textProperties, "What is my name?", nil)

		assert.Nil(t, err)
		assert.Equal(t, expected, *res)
	})

	t.Run("when the server has a an error", func(t *testing.T) {
		server := httptest.NewServer(&testAnswerHandler{
			t: t,
			answer: generateResponse{
				Error: "some error from the server",
			},
		})
		defer server.Close()

		c := New(server.URL, nullLogger())

		_, err := c.GenerateAllResults(context.Background(), textProperties, "What is my name?", nil)

		require.NotNil(t, err)
		assert.Error(t, err, "connection to OpenAI failed with status: 500 error: some error from the server")
	})
}

type testAnswerHandler struct {
	t *testing.T
	// the test handler will report as not ready before the time has passed
	answer generateResponse
}

func (f *testAnswerHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	assert.Equal(f.t, "/prompt", r.URL.String())
	assert.Equal(f.t, http.MethodPost, r.Method)

	if f.answer.Error != "" {
		outBytes, err := json.Marshal(f.answer)
		require.Nil(f.t, err)

		w.WriteHeader(http.StatusInternalServerError)
		w.Write(outBytes)
		return
	}

	bodyBytes, err := io.ReadAll(r.Body)
	require.Nil(f.t, err)
	defer r.Body.Close()

	var b map[string]interface{}
	require.Nil(f.t, json.Unmarshal(bodyBytes, &b))

	outBytes, err := json.Marshal(f.answer)
	require.Nil(f.t, err)

	w.Write(outBytes)
}

func ptString(in string) *string {
	return &in
}
