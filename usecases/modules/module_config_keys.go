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

package modules

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/modulecomponents"
)

// ValidateModuleConfigKeys rejects module settings that no module's own
// validation sees, so that a collection cannot use them to choose the host a
// module sends the server's provider key to:
//
//   - settings under an empty module name. Generative, reranker and other
//     additional-property modules read them in place of empty settings of
//     their own, so a module whose settings the update empties, or adds
//     empty, is rejected while they choose a host;
//   - host-choosing settings of a module that is not enabled, which would take
//     effect once the module is enabled.
//
// initial is nil on create. On update, class is the request body, and entries
// that initial already holds unchanged are not checked, so a stored class keeps
// accepting updates that leave its settings alone.
func (p *Provider) ValidateModuleConfigKeys(class, initial *models.Class) error {
	var initialModuleConfig any
	if initial != nil {
		initialModuleConfig = initial.ModuleConfig
	}
	if err := p.validateModuleKeys(class.ModuleConfig, initialModuleConfig); err != nil {
		return err
	}
	if err := p.validateUnnamedFallback(class.ModuleConfig, initialModuleConfig); err != nil {
		return err
	}
	for _, targetVector := range slices.Sorted(maps.Keys(class.VectorConfig)) {
		var initialVectorizer any
		if initial != nil {
			initialVectorizer = initial.VectorConfig[targetVector].Vectorizer
		}
		if err := p.validateModuleKeys(class.VectorConfig[targetVector].Vectorizer, initialVectorizer); err != nil {
			return fmt.Errorf("target vector %q: %w", targetVector, err)
		}
	}
	return nil
}

func (p *Provider) validateModuleKeys(moduleConfig, initialModuleConfig any) error {
	settings, ok := moduleConfig.(map[string]any)
	if !ok {
		return nil
	}
	initialSettings, _ := initialModuleConfig.(map[string]any)
	for _, key := range slices.Sorted(maps.Keys(settings)) {
		if stored, ok := initialSettings[key]; ok && p.sameJSON(stored, settings[key]) {
			continue
		}
		if key == "" {
			return fmt.Errorf("module config has settings under an empty module name")
		}
		if p.GetByName(key) == nil && modulecomponents.HasHostSetting(settings[key]) {
			return fmt.Errorf("module %q is not enabled, so its settings may not choose a host", key)
		}
	}
	return nil
}

// validateUnnamedFallback rejects an update that leaves a module with empty
// settings while settings stored under an empty module name choose a host: the
// module would read those in place of its own. An update cannot remove the
// stored entry, since the class-level module config is merged into the stored
// one.
func (p *Provider) validateUnnamedFallback(moduleConfig, initialModuleConfig any) error {
	settings, _ := moduleConfig.(map[string]any)
	initialSettings, _ := initialModuleConfig.(map[string]any)
	fallback, ok := settings[""]
	if !ok {
		fallback = initialSettings[""]
	}
	if !modulecomponents.HasHostSetting(fallback) {
		return nil
	}
	for _, key := range slices.Sorted(maps.Keys(settings)) {
		if key == "" {
			continue
		}
		if stored, ok := initialSettings[key]; ok && p.sameJSON(stored, settings[key]) {
			continue
		}
		if own, _ := settings[key].(map[string]any); len(own) == 0 {
			return fmt.Errorf("module %q has no settings of its own, so it would use the host stored under an empty module name: "+
				"give it its own settings", key)
		}
	}
	return nil
}

func (p *Provider) sameJSON(a, b any) bool {
	aJSON, errA := json.Marshal(a)
	bJSON, errB := json.Marshal(b)
	return errA == nil && errB == nil && string(aJSON) == string(bJSON)
}
