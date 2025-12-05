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

// Package main exports all module parameters to JSON for SDK developers.
// Usage: go run tools/export-parameters/main.go > docs/module-parameters.json
package main

import (
	"encoding/json"
	"fmt"
	"os"

	aws "github.com/weaviate/weaviate/modules/text2vec-aws/vectorizer"
	cohere "github.com/weaviate/weaviate/modules/text2vec-cohere/ent"
	google "github.com/weaviate/weaviate/modules/text2vec-google/vectorizer"
	huggingface "github.com/weaviate/weaviate/modules/text2vec-huggingface/ent"
	openai "github.com/weaviate/weaviate/modules/text2vec-openai/ent"
	transformers "github.com/weaviate/weaviate/modules/text2vec-transformers/vectorizer"
	"github.com/weaviate/weaviate/usecases/modulecomponents/settings"
)

// ModuleParameters represents a module's configuration parameters
type ModuleParameters struct {
	Name        string                          `json:"name"`
	Description string                          `json:"description"`
	Parameters  map[string]ExportedParameterDef `json:"parameters"`
}

// ExportedParameterDef is a JSON-friendly version of settings.ParameterDef
type ExportedParameterDef struct {
	JSONKey       string      `json:"jsonKey"`
	AlternateKeys []string    `json:"alternateKeys,omitempty"`
	DefaultValue  interface{} `json:"defaultValue,omitempty"`
	Description   string      `json:"description"`
	Required      bool        `json:"required"`
	AllowedValues interface{} `json:"allowedValues,omitempty"`
	DataType      string      `json:"dataType"`
}

func main() {
	// Collect parameters from all refactored modules
	modules := []ModuleParameters{
		{
			Name:        "text2vec-aws",
			Description: "AWS vectorizer module for text embeddings using Bedrock or Sagemaker",
			Parameters:  convertParameters(aws.Parameters),
		},
		{
			Name:        "text2vec-cohere",
			Description: "Cohere vectorizer module for text embeddings",
			Parameters:  convertParameters(cohere.Parameters),
		},
		{
			Name:        "text2vec-google",
			Description: "Google (Vertex AI & AI Studio) vectorizer module for text embeddings",
			Parameters:  convertParameters(google.Parameters),
		},
		{
			Name:        "text2vec-huggingface",
			Description: "HuggingFace vectorizer module for text embeddings",
			Parameters:  convertParameters(huggingface.Parameters),
		},
		{
			Name:        "text2vec-openai",
			Description: "OpenAI vectorizer module for text embeddings",
			Parameters:  convertParameters(openai.Parameters),
		},
		{
			Name:        "text2vec-transformers",
			Description: "Transformers vectorizer module for text embeddings using sentence-transformers",
			Parameters:  convertParameters(transformers.Parameters),
		},
		// Add more modules here as they're refactored
	}

	// Create output structure
	output := map[string]interface{}{
		"version":     "1.0.0",
		"generatedBy": "tools/export-parameters",
		"modules":     modules,
	}

	// Output as pretty-printed JSON
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(output); err != nil {
		fmt.Fprintf(os.Stderr, "Error encoding JSON: %v\n", err)
		os.Exit(1)
	}
}

// convertParameters converts internal ParameterDef map to exported format
func convertParameters(params map[string]settings.ParameterDef) map[string]ExportedParameterDef {
	result := make(map[string]ExportedParameterDef)

	for key, param := range params {
		result[key] = ExportedParameterDef{
			JSONKey:       param.JSONKey,
			AlternateKeys: param.AlternateKeys,
			DefaultValue:  param.DefaultValue,
			Description:   param.Description,
			Required:      param.Required,
			AllowedValues: param.AllowedValues,
			DataType:      param.DataType,
		}
	}

	return result
}
