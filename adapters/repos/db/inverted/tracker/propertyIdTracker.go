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
	"log"
	"os"
	"sync"
)

type JsonPropertyIdTracker struct {
	path        string
	LastId      uint64
	PropertyIds map[string]uint64
	sync.Mutex
}

func NewJsonPropertyIdTracker(path string) (*JsonPropertyIdTracker, error) {
	t := &JsonPropertyIdTracker{
		path:        path,
		PropertyIds: make(map[string]uint64),
	}

	log.Printf("Loading property id tracker from %v\n", path)

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
	t.path = path
	if t.LastId == 0 {
		t.LastId = 1
	}

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

	filename := t.path
	if flushBackup {
		filename = t.path + ".bak"
	}

	err = os.WriteFile(filename, bytes, 0o666)
	if err != nil {
		return err
	}
	return nil
}

// Closes the tracker and removes the backup file
func (t *JsonPropertyIdTracker) Close() error {
	if err := t.Flush(false); err != nil {
		return fmt.Errorf("flush before closing: %w", err)
	}

	t.Lock()
	defer t.Unlock()

	return nil
}

// Drop removes the tracker from disk
func (t *JsonPropertyIdTracker) Drop() error {
	t.Close()

	t.Lock()
	defer t.Unlock()

	if err := os.Remove(t.path); err != nil {
		return fmt.Errorf("remove prop length tracker state from disk:%v, %v", t.path, err)
	}
	if err := os.Remove(t.path + ".bak"); err != nil {
		return fmt.Errorf("remove prop length tracker state from disk:%v, %v", t.path+".bak", err)
	}

	return nil
}

func (t *JsonPropertyIdTracker) GetIdForProperty(property string) (uint64, error) {
	if t == nil {
		return 0, fmt.Errorf("property id tracker not initialised\n")
	}

	t.Lock()
	defer t.Unlock()

	if id, ok := t.PropertyIds[property]; ok {
		return id, nil
	}

	// panic(fmt.Sprintf("property %v not found\n", property))
	log.Printf("FIXME: property %v not created before use!\n", property)
	return t._createProperty(property)
	// return 0, fmt.Errorf("property %v not found\n", property)
}

func (t *JsonPropertyIdTracker) CreateProperty(property string) (uint64, error) {
	t.Lock()
	defer t.Unlock()

	return t._createProperty(property)
}

func (t *JsonPropertyIdTracker) _createProperty(property string) (uint64, error) {
	fmt.Printf("Creating property %v\n", property)

	if id, ok := t.PropertyIds[property]; ok {
		fmt.Printf("property %v already exists\n", property)
		return id, fmt.Errorf("property %v already exists\n", property)
	}

	t.LastId++
	t.PropertyIds[property] = t.LastId

	fmt.Printf("Created property %v with id %v\n", property, t.LastId)

	return t.LastId, nil
}
