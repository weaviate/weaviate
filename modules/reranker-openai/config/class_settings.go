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
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/moduletools"
	basesettings "github.com/weaviate/weaviate/usecases/modulecomponents/settings"
)

const (
	ModuleName     = "reranker-openai"
	DefaultBaseURL = "https://api.openai.com"
	DefaultModel   = "gpt-6-luna"
)

type classSettings struct {
	cfg                  moduletools.ClassConfig
	propertyValuesHelper basesettings.PropertyValuesHelper
}

func NewClassSettings(cfg moduletools.ClassConfig) *classSettings {
	return &classSettings{cfg: cfg, propertyValuesHelper: basesettings.NewPropertyValuesHelper(ModuleName)}
}

// CheckSettings checks the stored model and baseURL. The client runs it at
// query time, since a stored value of another type reads as the default.
func (cs *classSettings) CheckSettings() error {
	if cs.cfg == nil {
		// we would receive a nil-config on cross-class requests, such as Explore{}
		return errors.New("empty config")
	}

	settings := cs.rawSettings()
	for _, name := range []string{"model", "baseURL"} {
		v, ok := settings[name]
		if !ok || v == nil {
			continue
		}
		s, ok := v.(string)
		if !ok {
			return fmt.Errorf("%s must be a string, got %T", name, v)
		}
		if s == "" {
			return fmt.Errorf("%s must be a non-empty string", name)
		}
		if name == "baseURL" {
			if _, err := NormalizeBaseURL(s); err != nil {
				return fmt.Errorf("baseURL %w", err)
			}
		}
	}
	return nil
}

func (cs *classSettings) Validate(class *models.Class) error {
	if err := cs.CheckSettings(); err != nil {
		return err
	}
	return cs.propertyValuesHelper.ValidateBaseURL(cs.BaseURL())
}

// The client appends /v1 itself. A URL with a port and no host, such as
// https://:443, dials the local host.
func NormalizeBaseURL(baseURL string) (string, error) {
	u, err := url.Parse(baseURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" {
		return "", fmt.Errorf("must be an absolute http or https URL, got %q", baseURL)
	}
	p := strings.TrimSuffix(strings.TrimSuffix(u.EscapedPath(), "/"), "/v1")
	u.RawPath = p
	u.Path, _ = url.PathUnescape(p)
	return u.String(), nil
}

// rawSettings mirrors the helper's getters, which PropertyValuesHelper does not expose.
func (cs *classSettings) rawSettings() map[string]any {
	if settings := cs.cfg.ClassByModuleName(ModuleName); len(settings) > 0 {
		return settings
	}
	return cs.cfg.Class()
}

// A value that does not normalise is returned unchanged; Validate rejects it
// at class create and the client at query time.
func (cs *classSettings) BaseURL() string {
	raw := cs.getStringProperty("baseURL", DefaultBaseURL)
	if normalised, err := NormalizeBaseURL(raw); err == nil {
		return normalised
	}
	return raw
}

func (cs *classSettings) Model() string {
	return cs.getStringProperty("model", DefaultModel)
}

func (cs *classSettings) getStringProperty(name string, defaultValue string) string {
	return cs.propertyValuesHelper.GetPropertyAsStringWithNotExists(cs.cfg, name, defaultValue, defaultValue)
}
