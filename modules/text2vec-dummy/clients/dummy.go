//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package clients

import (
	"context"
	"crypto/sha256"
	"math/rand"
	"net/http"
	"time"

	"github.com/weaviate/weaviate/entities/moduletools"

	"github.com/weaviate/weaviate/usecases/modulecomponents"

	"github.com/sirupsen/logrus"
)

type vectorizer struct {
	httpClient *http.Client
	logger     logrus.FieldLogger
}

const (
	defaultRPM = 100_000
	defaultTPM = 200_000_000
)

func New(timeout time.Duration, logger logrus.FieldLogger) *vectorizer {
	return &vectorizer{
		httpClient: &http.Client{
			Timeout: timeout,
		},
		logger: logger,
	}
}

func (v *vectorizer) Vectorize(ctx context.Context, input []string, cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationResult[[]float32], *modulecomponents.RateLimits, int, error) {
	res, usage, err := v.vectorize(ctx, input)
	return res, nil, usage, err
}

func (v *vectorizer) VectorizeQuery(ctx context.Context, input []string,
	cfg moduletools.ClassConfig,
) (*modulecomponents.VectorizationResult[[]float32], error) {
	res, _, err := v.vectorize(ctx, input)
	return res, err
}

func (v *vectorizer) vectorize(ctx context.Context, input []string) (*modulecomponents.VectorizationResult[[]float32], int, error) {
	start := time.Now()
	r := rand.New(rand.NewSource(start.UnixNano()))
	randomSlice := make([]float32, 128)
	for i := range randomSlice {
		randomSlice[i] = r.Float32()
	}

	inputChars := 0
	vectors := make([][]float32, len(input))
	for i := range input {
		inputChars += len(input[i])
		shift := r.Float32()
		vectors[i] = make([]float32, 128)
		for j := range vectors[i] {
			vectors[i][j] = randomSlice[j] + shift
		}
	}
	time.Sleep(time.Duration(len(input))*100*time.Millisecond - time.Since(start))

	return &modulecomponents.VectorizationResult[[]float32]{
		Text:       input,
		Dimensions: len(vectors),
		Vector:     vectors,
	}, inputChars, nil
}

func (v *vectorizer) getApiKey(ctx context.Context) (string, error) {
	return "super-secret-key", nil
}

func (v *vectorizer) GetApiKeyHash(ctx context.Context, cfg moduletools.ClassConfig) [32]byte {
	key, err := v.getApiKey(ctx)
	if err != nil {
		return [32]byte{}
	}
	return sha256.Sum256([]byte(key))
}

func (v *vectorizer) GetVectorizerRateLimit(ctx context.Context, cfg moduletools.ClassConfig) *modulecomponents.RateLimits {
	rpm, tpm := modulecomponents.GetRateLimitFromContext(ctx, "Dummy", defaultRPM, defaultTPM)

	execAfterRequestFunction := func(limits *modulecomponents.RateLimits, tokensUsed int, deductRequest bool) {
		// refresh is after 60 seconds but leave a bit of room for errors. Otherwise, we only deduct the request that just happened
		if limits.LastOverwrite.Add(61 * time.Second).After(time.Now()) {
			if deductRequest {
				limits.RemainingRequests -= 1
			}
			limits.RemainingTokens -= tokensUsed
			return
		}

		limits.RemainingRequests = rpm
		limits.ResetRequests = time.Now().Add(time.Duration(61) * time.Second)
		limits.LimitRequests = rpm
		limits.LastOverwrite = time.Now()

		limits.RemainingTokens = tpm
		limits.LimitTokens = tpm
		limits.ResetTokens = time.Now().Add(time.Duration(61) * time.Second)
	}

	initialRL := &modulecomponents.RateLimits{AfterRequestFunction: execAfterRequestFunction, LastOverwrite: time.Now().Add(-61 * time.Minute)}
	initialRL.ResetAfterRequestFunction(0) // set initial values

	return initialRL
}
