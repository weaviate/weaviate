package stopwords

import (
	"sync"

	"github.com/pkg/errors"
)

type Detector struct {
	sync.Mutex
	stopwords map[string]struct{}
}

func NewDetectorFromPreset(preset string) (*Detector, error) {
	var list []string
	var ok bool

	if preset != "" {
		list, ok = presets[preset]
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

	return d, nil
}

func (d *Detector) SetAdditions(list []string) {
	d.Lock()
	defer d.Unlock()

	for _, word := range list {
		d.stopwords[word] = struct{}{}
	}
}

func (d *Detector) SetRemovals(list []string) {
	d.Lock()
	defer d.Unlock()

	for _, word := range list {
		delete(d.stopwords, word)
	}
}

func (d *Detector) IsStopword(word string) bool {
	d.Lock()
	defer d.Unlock()

	_, ok := d.stopwords[word]
	return ok
}
