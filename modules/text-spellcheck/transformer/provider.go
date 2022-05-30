//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package texttransformer

import (
	"github.com/semi-technologies/weaviate/entities/modulecapabilities"
)

type TextTransformerProvider struct {
	autocorrecProvider modulecapabilities.TextTransform
}

func New(autocorrecProvider modulecapabilities.TextTransform) *TextTransformerProvider {
	return &TextTransformerProvider{autocorrecProvider}
}

func (p *TextTransformerProvider) TextTransformers() map[string]modulecapabilities.TextTransform {
	textTransformers := map[string]modulecapabilities.TextTransform{}
	textTransformers["nearText"] = p.autocorrecProvider
	textTransformers["ask"] = p.autocorrecProvider
	return textTransformers
}
