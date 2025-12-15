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

package stopwords

import (
	"sync"

	"github.com/weaviate/weaviate/entities/models"

	"github.com/pkg/errors"
)

type StopwordDetector interface {
	IsStopword(string) bool
}

type Detector struct {
	sync.RWMutex
	preset    string
	stopwords map[string]struct{}
}

func NewDetectorFromConfig(config models.StopwordConfig) (*Detector, error) {
	d, err := NewDetectorFromPreset(config.Preset)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create new detector from config")
	}

	d.SetAdditions(config.Additions)
	d.SetRemovals(config.Removals)
	d.preset = config.Preset

	return d, nil
}

func NewDetectorFromPreset(preset string) (*Detector, error) {
	var list []string
	var ok bool

	if preset != "" {
		list, ok = Presets[preset]
		if !ok {
			return nil, errors.Errorf("preset %q not known to stopword detector", preset)
		}
	}

	d := &Detector{
		stopwords: map[string]struct{}{},
	}

	for _, word := range list {
		d.stopwords[word] = struct{}{}
	}
	d.preset = preset
	return d, nil
}

func (d *Detector) ReplaceDetectorFromConfig(config models.StopwordConfig) error {
	d.Lock()
	defer d.Unlock()

	stopwords := map[string]struct{}{}
	if config.Preset != "" {
		list, ok := Presets[config.Preset]
		if !ok {
			return errors.Errorf("preset %q not known to stopword detector", config.Preset)
		}
		for _, word := range list {
			stopwords[word] = struct{}{}
		}
	}

	for _, word := range config.Additions {
		stopwords[word] = struct{}{}
	}

	for _, word := range config.Removals {
		delete(stopwords, word)
	}

	d.stopwords = stopwords
	d.preset = config.Preset
	return nil
}

func (d *Detector) SetAdditions(additions []string) {
	d.Lock()
	defer d.Unlock()

	for _, add := range additions {
		d.stopwords[add] = struct{}{}
	}
}

func (d *Detector) SetRemovals(removals []string) {
	d.Lock()
	defer d.Unlock()

	for _, rem := range removals {
		delete(d.stopwords, rem)
	}
}

func (d *Detector) IsStopword(word string) bool {
	d.RLock()
	defer d.RUnlock()

	_, ok := d.stopwords[word]
	return ok
}

func (d *Detector) Preset() string {
	d.RLock()
	defer d.RUnlock()
	return d.preset
}
