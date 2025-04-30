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

package lsmkv

import (
	"encoding/binary"
	"io"
	"path/filepath"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringsetrange"
)

func SerializeMemtable(m *Memtable, w io.Writer) error {
	if err := binary.Write(w, binary.LittleEndian, m.size); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, m.createdAt.UnixNano()); err != nil {
		return err
	}

	// Serialize secondaryToPrimary
	if err := binary.Write(w, binary.LittleEndian, uint32(len(m.secondaryToPrimary))); err != nil {
		return err
	}
	for _, mp := range m.secondaryToPrimary {
		if err := binary.Write(w, binary.LittleEndian, uint32(len(mp))); err != nil {
			return err
		}
		for k, v := range mp {
			if err := writeBytes(w, []byte(k)); err != nil {
				return err
			}
			if err := writeBytes(w, v); err != nil {
				return err
			}
		}
	}

	if err := serializeBinarySearchTree(m.key, w); err != nil {
		return err
	}

	if err := serializeSearchTreeMulti(m.keyMulti, w); err != nil {
		return err
	}

	if err := serializeSearchTreeMap(m.keyMap, w); err != nil {
		return err
	}

	if err := serializeBinarySearchTree(m.primaryIndex, w); err != nil {
		return err
	}

	if err := roaringset.SerializeBinarySearchTree(m.roaringSet, w); err != nil {
		return err
	}

	if err := roaringsetrange.SerializeMemtable(m.roaringSetRange, w); err != nil {
		return err
	}

	return nil
}

func DeserializeMemtable(r io.Reader, path string, strategy string, secondaryIndices uint16,
	enableChecksumValidation bool, cl *commitLogger, metrics *Metrics, logger logrus.FieldLogger,
) (*Memtable, error) {
	m := &Memtable{
		path:                     path,
		strategy:                 strategy,
		secondaryIndices:         secondaryIndices,
		enableChecksumValidation: enableChecksumValidation,
		commitlog:                cl,
		dirtyAt:                  time.Now(),
		metrics:                  newMemtableMetrics(metrics, filepath.Dir(path), strategy),
	}

	err := binary.Read(r, binary.LittleEndian, &m.size)
	if err != nil {
		return nil, err
	}

	var createdNano int64
	err = binary.Read(r, binary.LittleEndian, &createdNano)
	if err != nil {
		return nil, err
	}
	m.createdAt = time.Unix(0, createdNano)

	// SecondaryToPrimary
	var outerLen uint32
	err = binary.Read(r, binary.LittleEndian, &outerLen)
	if err != nil {
		return nil, err
	}

	m.secondaryToPrimary = make([]map[string][]byte, outerLen)

	for i := uint32(0); i < outerLen; i++ {
		var innerLen uint32

		if err := binary.Read(r, binary.LittleEndian, &innerLen); err != nil {
			return nil, err
		}

		mp := make(map[string][]byte)

		for j := uint32(0); j < innerLen; j++ {
			k, err := readBytes(r)
			if err != nil {
				return nil, err
			}

			v, err := readBytes(r)
			if err != nil {
				return nil, err
			}

			mp[string(k)] = v
		}

		m.secondaryToPrimary[i] = mp
	}

	m.key, err = deserializeBinarySearchTree(r)
	if err != nil {
		return nil, err
	}

	m.keyMulti, err = deserializeSearchTreeMulti(r)
	if err != nil {
		return nil, err
	}

	m.keyMap, err = deserializeSearchTreeMap(r)
	if err != nil {
		return nil, err
	}

	m.primaryIndex, err = deserializeBinarySearchTree(r)
	if err != nil {
		return nil, err
	}

	m.roaringSet, err = roaringset.DeserializeBinarySearchTree(r)
	if err != nil {
		return nil, err
	}

	m.roaringSetRange, err = roaringsetrange.DeserializeMemtable(r, logger)
	if err != nil {
		return nil, err
	}

	m.metrics.size(m.size)

	return m, nil
}
