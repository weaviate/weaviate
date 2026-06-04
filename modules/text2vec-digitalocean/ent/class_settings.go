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

package ent

import (
	"context"
	"fmt"
	"os"

	"github.com/pkg/errors"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	basesettings "github.com/weaviate/weaviate/usecases/modulecomponents/settings"
)

const (
	DefaultBaseURL               = "https://inference.do-ai.run"
	DefaultVectorizeClassName    = true
	DefaultPropertyIndexed       = true
	DefaultVectorizePropertyName = false
	LowerCaseInput               = false
)

// ModelLister returns the set of available model ids from a DigitalOcean
// Serverless Inference endpoint. It is implemented in the clients package and
// injected here through DefaultModelLister to keep ent free of HTTP-client
// dependencies. Tests can override DefaultModelLister with a fake.
type ModelLister interface {
	ListModels(ctx context.Context, baseURL, apiKey string, weaviateUUID string) ([]string, error)
}

// DefaultModelLister is the lister used by Validate. It is overridable so that
// the clients package can register a real HTTP-backed implementation at init
// time and so that tests can inject fakes without spinning up an httptest
// server.
var DefaultModelLister ModelLister

type classSettings struct {
	*basesettings.BaseClassSettings
	cfg moduletools.ClassConfig
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{cfg: cfg, BaseClassSettings: basesettings.NewBaseClassSettings(cfg, LowerCaseInput)}
}

func (cs *classSettings) Model() string {
	return cs.GetPropertyAsString("model", "")
}

func (cs *classSettings) BaseURL() string {
	return cs.GetPropertyAsString("baseURL", DefaultBaseURL)
}

// Dimensions is currently disabled: DigitalOcean's Serverless Inference
// /v1/embeddings endpoint does not accept a "dimensions" request field
// (https://docs.digitalocean.com/reference/api/reference/serverless-inference/),
// even for MRL-capable models like qwen3-embedding-0.6b that could otherwise
// be truncated from their native 1024-d output. Sending the parameter is
// silently ignored upstream, so we drop it here to avoid misleading users
// who set it in the class config. Re-enable by returning
// cs.GetPropertyAsInt64("dimensions", nil) once DigitalOcean adds support.
func (cs *classSettings) Dimensions() *int64 {
	return nil
}

// apiKey resolves the DigitalOcean API key from the DIGITALOCEAN_APIKEY
// environment variable. Validation runs at collection-create time, where the
// per-request header is not available, so we only consult the server-level
// env var here. If unset, Validate will skip the model-list check rather than
// fail outright.
func (cs *classSettings) apiKey() string {
	return os.Getenv("DIGITALOCEAN_APIKEY")
}

func (cs *classSettings) WeaviateUUID() string {
	return os.Getenv("VECTOR_DB_UUID")
}

func (cs *classSettings) Validate(ctx context.Context, class *models.Class) error {
	if err := cs.BaseClassSettings.Validate(class); err != nil {
		return err
	}
	if err := cs.ValidateBaseURL(cs.BaseURL()); err != nil {
		return err
	}

	model := cs.Model()
	if model == "" {
		return errors.New("model must be set; choose one from GET /v1/models on the DigitalOcean Serverless Inference endpoint")
	}

	lister := DefaultModelLister
	if lister == nil {
		// no lister registered (should not happen in production because the
		// clients package init registers one); fall back to accepting any
		// model rather than failing validation outright
		return nil
	}

	apiKey := cs.apiKey()
	if apiKey == "" {
		// No server-side API key available, so we can't pre-validate the model
		// against /v1/models. Skip validation and rely on the Serverless
		// Inference endpoint to reject an unknown model at vectorize time,
		// where users can supply their own key via X-Digitalocean-Api-Key.
		return nil
	}

	available, err := lister.ListModels(ctx, cs.BaseURL(), apiKey, cs.WeaviateUUID())
	if err != nil {
		return errors.Wrap(err, "list DigitalOcean models")
	}

	for _, id := range available {
		if id == model {
			return nil
		}
	}

	return fmt.Errorf("model %q is not available on the DigitalOcean Serverless Inference endpoint; available models: %v", model, available)
}
