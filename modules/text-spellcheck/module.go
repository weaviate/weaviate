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

package modspellcheck

import (
	"context"
	"net/http"
	"os"
	"time"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/moduletools"
	spellcheckadditional "github.com/weaviate/weaviate/modules/text-spellcheck/additional"
	spellcheckadditionalspellcheck "github.com/weaviate/weaviate/modules/text-spellcheck/additional/spellcheck"
	"github.com/weaviate/weaviate/modules/text-spellcheck/clients"
	"github.com/weaviate/weaviate/modules/text-spellcheck/ent"
	spellchecktexttransformer "github.com/weaviate/weaviate/modules/text-spellcheck/transformer"
	spellchecktexttransformerautocorrect "github.com/weaviate/weaviate/modules/text-spellcheck/transformer/autocorrect"
)

func New() *SpellCheckModule {
	return &SpellCheckModule{}
}

type SpellCheckModule struct {
	spellCheck                   spellCheckClient
	additionalPropertiesProvider modulecapabilities.AdditionalProperties
	textTransformersProvider     modulecapabilities.TextTransformers
}

type spellCheckClient interface {
	Check(ctx context.Context, text []string) (*ent.SpellCheckResult, error)
	MetaInfo() (map[string]interface{}, error)
}

func (m *SpellCheckModule) Name() string {
	return "text-spellcheck"
}

func (m *SpellCheckModule) Type() modulecapabilities.ModuleType {
	return modulecapabilities.Extension
}

func (m *SpellCheckModule) Init(ctx context.Context,
	params moduletools.ModuleInitParams,
) error {
	uri := os.Getenv("SPELLCHECK_INFERENCE_API")
	if uri == "" {
		return errors.Errorf("required variable SPELLCHECK_INFERENCE_API is not set")
	}

	client := clients.New(uri, params.GetConfig().ModuleHttpClientTimeout, params.GetLogger())

	if err := client.WaitForStartup(ctx, 1*time.Second); err != nil {
		return errors.Wrap(err, "init remote spell check module")
	}

	m.spellCheck = client

	m.initTextTransformers()
	m.initAdditional()

	return nil
}

func (m *SpellCheckModule) initTextTransformers() {
	autocorrectProvider := spellchecktexttransformerautocorrect.New(m.spellCheck)
	m.textTransformersProvider = spellchecktexttransformer.New(autocorrectProvider)
}

func (m *SpellCheckModule) initAdditional() {
	spellCheckProvider := spellcheckadditionalspellcheck.New(m.spellCheck)
	m.additionalPropertiesProvider = spellcheckadditional.New(spellCheckProvider)
}

func (m *SpellCheckModule) RootHandler() http.Handler {
	// TODO: remove once this is a capability interface
	return nil
}

func (m *SpellCheckModule) MetaInfo() (map[string]interface{}, error) {
	return m.spellCheck.MetaInfo()
}

func (m *SpellCheckModule) AdditionalProperties() map[string]modulecapabilities.AdditionalProperty {
	return m.additionalPropertiesProvider.AdditionalProperties()
}

func (m *SpellCheckModule) TextTransformers() map[string]modulecapabilities.TextTransform {
	return m.textTransformersProvider.TextTransformers()
}

// verify we implement the modules.Module interface
var (
	_ = modulecapabilities.Module(New())
	_ = modulecapabilities.AdditionalProperties(New())
	_ = modulecapabilities.MetaProvider(New())
	_ = modulecapabilities.TextTransformers(New())
)
