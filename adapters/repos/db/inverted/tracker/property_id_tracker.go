//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package tracker

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"errors"
)

type JsonPropertyIdTracker struct {
	Path        string
	LastId      uint64
	PropertyIds map[string]uint64
	sync.Mutex
}

func NewJsonPropertyIdTracker(path string) (*JsonPropertyIdTracker, error) {
	t := &JsonPropertyIdTracker{
		Path:        path,
		PropertyIds: make(map[string]uint64),
		LastId:      0,
	}

	// read the file into memory
	bytes, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			t.Flush(false)
			return t, nil
		}
		return nil, err
	}

	// Unmarshal the data

	if err := json.Unmarshal(bytes, &t); err != nil {
		return nil, err
	}
	t.Path = path

	return t, nil
}

// Writes the current state of the tracker to disk.  (flushBackup = true) will only write the backup file
func (t *JsonPropertyIdTracker) Flush(flushBackup bool) error {
	if !flushBackup { // Write the backup file first
		t.Flush(true)
	}

	t.Lock()
	defer t.Unlock()

	bytes, err := json.Marshal(t)
	if err != nil {
		return err
	}

	filename := t.Path
	if flushBackup {
		filename = t.Path + ".bak"
	}

	// Do a write+rename to avoid corrupting the file if we crash while writing
	tempfile := filename + ".tmp"

	err = os.WriteFile(tempfile, bytes, 0o666)
	if err != nil {
		return err
	}

	err = os.Rename(tempfile, filename)
	if err != nil {
		return err
	}

	return nil
}

// Drop removes the tracker from disk
func (t *JsonPropertyIdTracker) Drop() error {
	t.Lock()
	defer t.Unlock()

	if err := os.Remove(t.Path); err != nil {
		return fmt.Errorf("remove prop length tracker state from disk:%v, %w", t.Path, err)
	}
	if err := os.Remove(t.Path + ".bak"); err != nil {
		return fmt.Errorf("remove prop length tracker state from disk:%v, %w", t.Path+".bak", err)
	}

	return nil
}

// Closes the tracker and removes the backup file
func (t *JsonPropertyIdTracker) Close() error {
	if err := t.Flush(false); err != nil {
		return fmt.Errorf( "flush before closing: %w", err)
	}

	t.Lock()
	defer t.Unlock()

	t.PropertyIds = nil
	

	return nil
}

func (t *JsonPropertyIdTracker) GetIdForProperty(property string) uint64 {
	t.Lock()
	defer t.Unlock()

	if id, ok := t.PropertyIds[property]; ok {
		return id
	}

	id, _ := t.doCreateProperty(property)
	return id
}

func (t *JsonPropertyIdTracker) CreateProperty(property string) (uint64, error) {
	t.Lock()
	defer t.Unlock()

	return t.doCreateProperty(property)
}

func (t *JsonPropertyIdTracker) doCreateProperty(property string) (uint64, error) {
	if id, ok := t.PropertyIds[property]; ok {
		return id, fmt.Errorf("property %v already exists\n", property)
	}

	t.LastId++
	t.PropertyIds[property] = t.LastId

	return t.LastId, nil
}
