//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

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
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
)

func nullLogger() logrus.FieldLogger {
	l, _ := test.NewNullLogger()
	return l
}

func fakeBuildUrl(serverURL string) (string, error) {
	return serverURL, nil
}

func TestGetAnswer(t *testing.T) {
	textProperties := []map[string]string{{"prop": "My name is john"}}
	t.Run("when the server has a successful answer ", func(t *testing.T) {
		handler := &testAnswerHandler{
			t: t,
			answer: generateResponse{
				Choices: []choice{{
					FinishReason: "test",
					Index:        0,
					Logprobs:     "",
					Text:         "John",
				}},
				Error: nil,
			},
		}
		server := httptest.NewServer(handler)
		defer server.Close()

		c := New("databricksToken", 0, nullLogger())
		c.buildServingUrl = func(servingUrl string) (string, error) {
			return fakeBuildUrl(server.URL)
		}

		expected := modulecapabilities.GenerateResponse{
			Result: ptString("John"),
		}

		res, err := c.GenerateAllResults(context.Background(), textProperties, "What is my name?", nil, false, nil)

		assert.Nil(t, err)
		assert.Equal(t, expected, *res)
	})

	t.Run("when X-Databricks-Servingurl header is passed", func(t *testing.T) {
		settings := &fakeClassSettings{
			baseURL: "http://default-url.com",
		}
		c := New("databricksToken", 0, nullLogger())

		ctxWithValue := context.WithValue(context.Background(),
			"X-Databricks-Servingurl", []string{"http://base-url-passed-in-header.com"})

		buildURL, err := c.buildDatabricksServingUrl(ctxWithValue, settings)
		require.NoError(t, err)
		assert.Equal(t, "http://base-url-passed-in-header.com", buildURL)

		buildURL, err = c.buildDatabricksServingUrl(context.TODO(), settings)
		require.NoError(t, err)
		assert.Equal(t, "", buildURL)
	})
}

type testAnswerHandler struct {
	t *testing.T
	// the test handler will report as not ready before the time has passed
	answer generateResponse
}

func (f *testAnswerHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	assert.Equal(f.t, http.MethodPost, r.Method)

	if f.answer.Error != nil && f.answer.Error.Message != "" {
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

func TestOpenAIApiErrorDecode(t *testing.T) {
	t.Run("getModelStringQuery", func(t *testing.T) {
		type args struct {
			response []byte
		}
		tests := []struct {
			name string
			args args
			want string
		}{
			{
				name: "Error code: missing property",
				args: args{
					response: []byte(`{"message": "failed", "type": "error", "param": "arg..."}`),
				},
				want: "",
			},
			{
				name: "Error code: as int",
				args: args{
					response: []byte(`{"message": "failed", "type": "error", "param": "arg...", "code": 500}`),
				},
				want: "500",
			},
			{
				name: "Error code as string number",
				args: args{
					response: []byte(`{"message": "failed", "type": "error", "param": "arg...", "code": "500"}`),
				},
				want: "500",
			},
			{
				name: "Error code as string text",
				args: args{
					response: []byte(`{"message": "failed", "type": "error", "param": "arg...", "code": "invalid_api_key"}`),
				},
				want: "invalid_api_key",
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				var got *openAIApiError
				err := json.Unmarshal(tt.args.response, &got)
				require.NoError(t, err)

				if got.Code.String() != tt.want {
					t.Errorf("OpenAIerror.code = %v, want %v", got.Code, tt.want)
				}
			})
		}
	})
}

func ptString(in string) *string {
	return &in
}

type fakeClassSettings struct {
	isLegacy         bool
	model            string
	maxTokens        float64
	temperature      float64
	frequencyPenalty float64
	presencePenalty  float64
	topP             float64
	resourceName     string
	deploymentID     string
	isAzure          bool
	baseURL          string
	apiVersion       string
	servingUrl       string
}

func (s *fakeClassSettings) IsLegacy() bool {
	return s.isLegacy
}

func (s *fakeClassSettings) Model() string {
	return s.model
}

func (s *fakeClassSettings) MaxTokens() float64 {
	return s.maxTokens
}

func (s *fakeClassSettings) Temperature() float64 {
	return s.temperature
}

func (s *fakeClassSettings) FrequencyPenalty() float64 {
	return s.frequencyPenalty
}

func (s *fakeClassSettings) PresencePenalty() float64 {
	return s.presencePenalty
}

func (s *fakeClassSettings) TopP() float64 {
	return s.topP
}

func (s *fakeClassSettings) ResourceName() string {
	return s.resourceName
}

func (s *fakeClassSettings) DeploymentID() string {
	return s.deploymentID
}

func (s *fakeClassSettings) IsAzure() bool {
	return s.isAzure
}

func (s *fakeClassSettings) GetMaxTokensForModel(model string) float64 {
	return 0
}

func (s *fakeClassSettings) Validate(class *models.Class) error {
	return nil
}

func (s *fakeClassSettings) BaseURL() string {
	return s.baseURL
}

func (s *fakeClassSettings) ApiVersion() string {
	return s.apiVersion
}

func (s *fakeClassSettings) ServingURL() string {
	return s.servingUrl
}
