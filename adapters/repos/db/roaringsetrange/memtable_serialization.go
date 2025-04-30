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

package roaringsetrange

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/sirupsen/logrus"
)

func SerializeMemtable(m *Memtable, w io.Writer) error {
	// Write additions
	if err := binary.Write(w, binary.LittleEndian, uint64(len(m.additions))); err != nil {
		return err
	}
	for k, v := range m.additions {
		if err := binary.Write(w, binary.LittleEndian, k); err != nil {
			return err
		}
		if err := binary.Write(w, binary.LittleEndian, v); err != nil {
			return err
		}
	}

	// Write deletions
	if err := binary.Write(w, binary.LittleEndian, uint64(len(m.deletions))); err != nil {
		return err
	}
	for k := range m.deletions {
		if err := binary.Write(w, binary.LittleEndian, k); err != nil {
			return err
		}
	}

	return nil
}

func DeserializeMemtable(r io.Reader, logger logrus.FieldLogger) (*Memtable, error) {
	m := &Memtable{
		additions: make(map[uint64]uint64),
		deletions: make(map[uint64]struct{}),
	}

	var addCount uint64
	if err := binary.Read(r, binary.LittleEndian, &addCount); err != nil {
		return nil, fmt.Errorf("reading additions count: %w", err)
	}

	for i := uint64(0); i < addCount; i++ {
		var k, v uint64
		if err := binary.Read(r, binary.LittleEndian, &k); err != nil {
			return nil, fmt.Errorf("reading addition key: %w", err)
		}
		if err := binary.Read(r, binary.LittleEndian, &v); err != nil {
			return nil, fmt.Errorf("reading addition value: %w", err)
		}
		logger.WithFields(logrus.Fields{"key": k, "value": v}).Debug("Deserialized addition")
		m.additions[k] = v
	}

	var delCount uint64
	if err := binary.Read(r, binary.LittleEndian, &delCount); err != nil {
		return nil, fmt.Errorf("reading deletions count: %w", err)
	}
	for i := uint64(0); i < delCount; i++ {
		var k uint64
		if err := binary.Read(r, binary.LittleEndian, &k); err != nil {
			return nil, fmt.Errorf("reading deletion key: %w", err)
		}
		logger.WithField("key", k).Debug("Deserialized deletion")
		m.deletions[k] = struct{}{}
	}

	return m, nil
}
