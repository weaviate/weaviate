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

package hnsw

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
)

func writeStateTo(state *DeserializationResult, w io.Writer) error {
	// version
	if err := writeByte(w, 0); err != nil {
		return err
	}

	if err := writeUint64(w, state.Entrypoint); err != nil {
		return err
	}

	if err := writeUint16(w, state.Level); err != nil {
		return err
	}

	if err := writeBool(w, state.Compressed); err != nil {
		return err
	}

	if state.Compressed {
		if err := writeUint16(w, state.PQData.Ks); err != nil {
			return err
		}
		if err := writeUint16(w, state.PQData.M); err != nil {
			return err
		}
		if err := writeUint16(w, state.PQData.Dimensions); err != nil {
			return err
		}
		if err := writeByte(w, byte(state.PQData.EncoderType)); err != nil {
			return err
		}
		if err := writeByte(w, state.PQData.EncoderDistribution); err != nil {
			return err
		}
		if err := writeBool(w, state.PQData.UseBitsEncoding); err != nil {
			return err
		}

		for _, encoder := range state.PQData.Encoders {
			if _, err := w.Write(encoder.ExposeDataForRestore()); err != nil {
				return err
			}
		}
	}

	if err := writeUint32(w, uint32(len(state.Nodes))); err != nil {
		return err
	}

	for _, n := range state.Nodes {
		if n == nil {
			// nil node
			if err := writeByte(w, 0); err != nil {
				return err
			}
			continue
		}

		_, hasATombstone := state.Tombstones[n.id]
		if hasATombstone {
			if err := writeByte(w, 1); err != nil {
				return err
			}
		} else {
			if err := writeByte(w, 2); err != nil {
				return err
			}
		}

		if err := writeUint32(w, uint32(n.level)); err != nil {
			return err
		}

		if err := writeUint32(w, uint32(len(n.connections))); err != nil {
			return err
		}
		for _, ls := range n.connections {
			if err := writeUint32(w, uint32(len(ls))); err != nil {
				return err
			}
			for _, c := range ls {
				if err := writeUint64(w, c); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func readStateFrom(r io.Reader) (*DeserializationResult, error) {
	res := &DeserializationResult{
		NodesDeleted:      make(map[uint64]struct{}),
		Tombstones:        make(map[uint64]struct{}),
		TombstonesDeleted: make(map[uint64]struct{}),
		LinksReplaced:     make(map[uint64]map[uint16]struct{}),
	}

	var b [8]byte

	_, err := io.ReadFull(r, b[:1]) // version
	if err != nil {
		return nil, errors.Wrapf(err, "read version")
	}
	if b[0] != 0 {
		return nil, fmt.Errorf("unsupported version %d", b[0])
	}

	_, err = io.ReadFull(r, b[:8]) // entrypoint
	if err != nil {
		return nil, errors.Wrapf(err, "read entrypoint")
	}
	res.Entrypoint = binary.LittleEndian.Uint64(b[:8])

	_, err = io.ReadFull(r, b[:2]) // level
	if err != nil {
		return nil, errors.Wrapf(err, "read level")
	}
	res.Level = binary.LittleEndian.Uint16(b[:2])

	_, err = io.ReadFull(r, b[:1]) // compressed
	if err != nil {
		return nil, errors.Wrapf(err, "read compressed")
	}
	res.Compressed = b[0] == 1

	// PQ data
	if res.Compressed {
		_, err = io.ReadFull(r, b[:2]) // PQData.Ks
		if err != nil {
			return nil, errors.Wrapf(err, "read PQData.Ks")
		}
		ks := binary.LittleEndian.Uint16(b[:2])

		_, err = io.ReadFull(r, b[:2]) // PQData.M
		if err != nil {
			return nil, errors.Wrapf(err, "read PQData.M")
		}
		m := binary.LittleEndian.Uint16(b[:2])

		_, err = io.ReadFull(r, b[:2]) // PQData.Dimensions
		if err != nil {
			return nil, errors.Wrapf(err, "read PQData.Dimensions")
		}
		dims := binary.LittleEndian.Uint16(b[:2])

		_, err = io.ReadFull(r, b[:1]) // PQData.EncoderType
		if err != nil {
			return nil, errors.Wrapf(err, "read PQData.EncoderType")
		}
		encoderType := compressionhelpers.Encoder(b[0])

		_, err = io.ReadFull(r, b[:1]) // PQData.EncoderDistribution
		if err != nil {
			return nil, errors.Wrapf(err, "read PQData.EncoderDistribution")
		}
		dist := b[0]

		_, err = io.ReadFull(r, b[:1]) // PQData.UseBitsEncoding
		if err != nil {
			return nil, errors.Wrapf(err, "read PQData.UseBitsEncoding")
		}
		useBitsEncoding := b[0] == 1

		encoder := compressionhelpers.Encoder(encoderType)

		res.PQData = compressionhelpers.PQData{
			Dimensions:          dims,
			EncoderType:         encoder,
			Ks:                  ks,
			M:                   m,
			EncoderDistribution: dist,
			UseBitsEncoding:     useBitsEncoding,
		}

		var encoderReader func(io.Reader, *DeserializationResult, uint16) (compressionhelpers.PQEncoder, error)

		switch encoder {
		case compressionhelpers.UseTileEncoder:
			encoderReader = ReadTileEncoder
		case compressionhelpers.UseKMeansEncoder:
			encoderReader = ReadKMeansEncoder
		default:
			return nil, errors.New("unsuported encoder type")
		}

		for i := uint16(0); i < m; i++ {
			encoder, err := encoderReader(r, res, i)
			if err != nil {
				return nil, err
			}
			res.PQData.Encoders = append(res.PQData.Encoders, encoder)
		}
	}

	_, err = io.ReadFull(r, b[:4]) // nodes
	if err != nil {
		return nil, errors.Wrapf(err, "read nodes count")
	}
	nodesCount := int(binary.LittleEndian.Uint32(b[:4]))

	res.Nodes = make([]*vertex, nodesCount)

	for i := 0; i < nodesCount; i++ {
		_, err = io.ReadFull(r, b[:1]) // node existence
		if err != nil {
			return nil, errors.Wrapf(err, "read node existence")
		}
		if b[0] == 0 {
			// nil node
			continue
		}

		n := &vertex{id: uint64(i + 1)}

		if b[0] == 1 {
			res.Tombstones[n.id] = struct{}{}
		} else if b[0] != 2 {
			return nil, fmt.Errorf("unsupported node existence state")
		}

		_, err = io.ReadFull(r, b[:4]) // level
		if err != nil {
			return nil, errors.Wrapf(err, "read node level")
		}
		n.level = int(binary.LittleEndian.Uint32(b[:4]))

		_, err = io.ReadFull(r, b[:4]) // connections count
		if err != nil {
			return nil, errors.Wrapf(err, "read node connections count")
		}
		connCount := int(binary.LittleEndian.Uint32(b[:4]))

		if connCount > 0 {
			n.connections = make([][]uint64, connCount)

			for l := 0; l < connCount; l++ {
				_, err = io.ReadFull(r, b[:4]) // connections count at level
				if err != nil {
					return nil, errors.Wrapf(err, "read node connections count at level")
				}
				connCountAtLevel := int(binary.LittleEndian.Uint32(b[:4]))

				if connCountAtLevel > 0 {
					n.connections[l] = make([]uint64, connCountAtLevel)

					for c := 0; c < connCountAtLevel; c++ {
						_, err = io.ReadFull(r, b[:8]) // connection at level
						if err != nil {
							return nil, errors.Wrapf(err, "read node connection at level")
						}
						n.connections[l][c] = binary.LittleEndian.Uint64(b[:8])
					}
				}

			}
		}

		res.Nodes[i] = n
	}

	return res, nil
}
