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

package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/url"
	"strconv"
	"strings"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	"github.com/weaviate/weaviate/usecases/modulecomponents"
)

const moduleName = "decisions-openai"

const (
	DefaultBaseURL      = "https://api.openai.com"
	DefaultOpenAIModel  = "gpt-6-luna"
	DefaultMaxDocuments = 100
	// MaxDocumentsLimit bounds the per-query API spend: every document is
	// one request to the Decisions API.
	MaxDocumentsLimit = 1000

	// QuestionRelevance asks whether the document answers the rerank query.
	// QuestionStatement sends the rerank query as the predicate itself: a
	// statement about the document that the model judges true or false.
	QuestionRelevance = "relevance"
	QuestionStatement = "statement"
	DefaultQuestion   = QuestionRelevance
)

// Settings are the class settings with the defaults filled in.
type Settings struct {
	Model string
	// Question is what the model is asked about every document.
	Question string
	// BaseURL is the API endpoint without a trailing "/" or "/v1".
	BaseURL string
	// MaxDocuments is the highest number of documents a single rerank
	// request may send to the OpenAI API. Resolve does not check its range.
	MaxDocuments int
}

type classSettings struct {
	cfg moduletools.ClassConfig
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{cfg: cfg}
}

func (ic *classSettings) Validate(class *models.Class) error {
	if ic.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return errors.New("empty config")
	}

	settings, err := ic.Resolve()
	if err != nil {
		return err
	}
	if settings.MaxDocuments < 1 || settings.MaxDocuments > MaxDocumentsLimit {
		return fmt.Errorf("maxDocuments must be between 1 and %d, got %d", MaxDocumentsLimit, settings.MaxDocuments)
	}
	return modulecomponents.ValidateBaseURL(settings.BaseURL)
}

// Resolve reads the settings. A setting of the wrong type, an empty model, an
// unknown question and a baseURL without scheme or host are errors. A missing
// setting is its default.
func (ic *classSettings) Resolve() (Settings, error) {
	model, err := ic.stringSetting("model", DefaultOpenAIModel)
	if err != nil {
		return Settings{}, err
	}
	if model == "" {
		return Settings{}, errors.New("no model provided")
	}
	question, err := ic.stringSetting("question", DefaultQuestion)
	if err != nil {
		return Settings{}, err
	}
	if question != QuestionRelevance && question != QuestionStatement {
		return Settings{}, fmt.Errorf("question must be %q or %q, got %q", QuestionRelevance, QuestionStatement, question)
	}
	maxDocuments, err := ic.maxDocuments()
	if err != nil {
		return Settings{}, err
	}
	baseURL, err := ic.stringSetting("baseURL", DefaultBaseURL)
	if err != nil {
		return Settings{}, err
	}
	if baseURL == "" {
		baseURL = DefaultBaseURL
	}
	baseURL, err = NormalizeBaseURL("baseURL", baseURL)
	if err != nil {
		return Settings{}, err
	}
	return Settings{Model: model, Question: question, BaseURL: baseURL, MaxDocuments: maxDocuments}, nil
}

// NormalizeBaseURL removes a trailing "/" and a trailing "/v1" from baseURL,
// so that both "https://host" and the OpenAI SDK form "https://host/v1" name
// the same endpoint. source names the setting or header in the error.
func NormalizeBaseURL(source, baseURL string) (string, error) {
	parsed, err := url.Parse(baseURL)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Hostname() == "" {
		return "", fmt.Errorf("%s must be a URL with an http or https scheme and a host, such as %s, got %q",
			source, DefaultBaseURL, baseURL)
	}
	parsed.Path = strings.TrimSuffix(strings.TrimRight(parsed.Path, "/"), "/v1")
	parsed.RawPath = ""
	return parsed.String(), nil
}

func (ic *classSettings) setting(name string) any {
	if ic.cfg == nil {
		return nil
	}
	settings := ic.cfg.ClassByModuleName(moduleName)
	if len(settings) == 0 {
		settings = ic.cfg.Class()
	}
	return settings[name]
}

func (ic *classSettings) stringSetting(name, defaultValue string) (string, error) {
	switch value := ic.setting(name).(type) {
	case nil:
		return defaultValue, nil
	case string:
		return value, nil
	default:
		return "", fmt.Errorf("%s must be a string, got %s", name, typeName(value))
	}
}

func (ic *classSettings) maxDocuments() (int, error) {
	value := ic.setting("maxDocuments")
	notWhole := func() (int, error) {
		shown := fmt.Sprintf("%v", value)
		if text, ok := value.(string); ok {
			shown = strconv.Quote(text)
		}
		return 0, fmt.Errorf("maxDocuments must be a whole number between 1 and %d, got %s", MaxDocumentsLimit, shown)
	}
	fromFloat := func(number float64) (int, error) {
		// The bound also keeps the conversion to int defined.
		if number != math.Trunc(number) || math.Abs(number) > math.MaxInt32 {
			return notWhole()
		}
		return int(number), nil
	}
	switch v := value.(type) {
	case nil:
		return DefaultMaxDocuments, nil
	case int:
		return v, nil
	case int16:
		return int(v), nil
	case int32:
		return int(v), nil
	case int64:
		return fromFloat(float64(v))
	case float32:
		return fromFloat(float64(v))
	case float64:
		return fromFloat(v)
	case json.Number:
		number, err := v.Float64()
		if err != nil {
			return notWhole()
		}
		return fromFloat(number)
	case string:
		// Accepted because the shared settings helper accepts it.
		number, err := strconv.Atoi(v)
		if err != nil {
			return notWhole()
		}
		return number, nil
	default:
		return notWhole()
	}
}

func typeName(value any) string {
	switch value.(type) {
	case bool:
		return "a boolean"
	case json.Number, float32, float64, int, int16, int32, int64:
		return "a number"
	case []any, []string:
		return "a list"
	case map[string]any:
		return "an object"
	default:
		return fmt.Sprintf("%T", value)
	}
}
