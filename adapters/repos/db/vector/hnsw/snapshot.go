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
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

const checkpointChunkSize = 100_000

// returns checkpoints which can be used as parallelizatio hints
func writeStateTo(state *DeserializationResult, w io.Writer) ([]Checkpoint, error) {
	// version
	offset := 0
	if err := writeByte(w, 0); err != nil {
		return nil, err
	}
	offset += writeByteSize

	if err := writeUint64(w, state.Entrypoint); err != nil {
		return nil, err
	}
	offset += writeUint64Size

	if err := writeUint16(w, state.Level); err != nil {
		return nil, err
	}
	offset += writeUint16Size

	if err := writeBool(w, state.Compressed); err != nil {
		return nil, err
	}
	offset += writeByteSize

	if state.Compressed {
		if err := writeUint16(w, state.PQData.Ks); err != nil {
			return nil, err
		}
		offset += writeUint16Size

		if err := writeUint16(w, state.PQData.M); err != nil {
			return nil, err
		}
		offset += writeUint16Size

		if err := writeUint16(w, state.PQData.Dimensions); err != nil {
			return nil, err
		}
		offset += writeUint16Size

		if err := writeByte(w, byte(state.PQData.EncoderType)); err != nil {
			return nil, err
		}
		offset += writeByteSize

		if err := writeByte(w, state.PQData.EncoderDistribution); err != nil {
			return nil, err
		}
		offset += writeByteSize

		if err := writeBool(w, state.PQData.UseBitsEncoding); err != nil {
			return nil, err
		}
		offset += writeByteSize

		for _, encoder := range state.PQData.Encoders {
			if n, err := w.Write(encoder.ExposeDataForRestore()); err != nil {
				return nil, err
			} else {
				offset += n
			}
		}
	}

	if err := writeUint32(w, uint32(len(state.Nodes))); err != nil {
		return nil, err
	}
	offset += writeUint32Size

	var checkpoints []Checkpoint
	// start at the very first node
	checkpoints = append(checkpoints, Checkpoint{NodeID: 0, Offset: uint64(offset)})

	nonNilNodes := 0

	for i, n := range state.Nodes {
		if n == nil {
			// nil node
			if err := writeByte(w, 0); err != nil {
				return nil, err
			}
			offset += writeByteSize
			continue
		}

		if nonNilNodes%checkpointChunkSize == 0 && nonNilNodes > 0 {
			checkpoints = append(checkpoints, Checkpoint{NodeID: uint64(i), Offset: uint64(offset)})
		}

		_, hasATombstone := state.Tombstones[n.id]
		if hasATombstone {
			if err := writeByte(w, 1); err != nil {
				return nil, err
			}
		} else {
			if err := writeByte(w, 2); err != nil {
				return nil, err
			}
		}
		offset += writeByteSize

		if err := writeUint32(w, uint32(n.level)); err != nil {
			return nil, err
		}
		offset += writeUint32Size

		if err := writeUint32(w, uint32(len(n.connections))); err != nil {
			return nil, err
		}
		offset += writeUint32Size

		for _, ls := range n.connections {
			if err := writeUint32(w, uint32(len(ls))); err != nil {
				return nil, err
			}
			offset += writeUint32Size

			for _, c := range ls {
				if err := writeUint64(w, c); err != nil {
					return nil, err
				}
				offset += writeUint64Size
			}
		}

		nonNilNodes++
	}

	// note that we are not adding an end checkpoint, so the implicit contract
	// here is that the reader must read from the last checkpoint to EOF.
	return checkpoints, nil
}

func readStateFrom(f *os.File, concurrency int, checkpoints []Checkpoint,
	logger logrus.FieldLogger,
) (*DeserializationResult, error) {
	res := &DeserializationResult{
		NodesDeleted:      make(map[uint64]struct{}),
		Tombstones:        make(map[uint64]struct{}),
		TombstonesDeleted: make(map[uint64]struct{}),
		LinksReplaced:     make(map[uint64]map[uint16]struct{}),
	}

	// start with a single-threaded reader until we make it the nodes section
	r := bufio.NewReader(f)

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

	eg := enterrors.NewErrorGroupWrapper(logger)
	eg.SetLimit(concurrency)
	for cpPos, cp := range checkpoints {
		start := int(cp.Offset)
		var end int
		if cpPos != len(checkpoints)-1 {
			end = int(checkpoints[cpPos+1].Offset)
		} else {
			st, err := f.Stat()
			if err != nil {
				return nil, errors.Wrapf(err, "get file stat")
			}

			end = int(st.Size())
		}

		eg.Go(func() error {
			var b [8]byte
			var read int

			currNodeID := cp.NodeID
			sr := io.NewSectionReader(f, int64(start), int64(end-start))
			r := bufio.NewReader(sr)

			for read < end-start {
				n, err := io.ReadFull(r, b[:1]) // node existence
				if err != nil {
					return errors.Wrapf(err, "read node existence")
				}
				read += n
				if b[0] == 0 {
					// nil node
					currNodeID++
					continue
				}

				node := &vertex{id: currNodeID}

				if b[0] == 1 {
					res.Tombstones[node.id] = struct{}{}
				} else if b[0] != 2 {
					return fmt.Errorf("unsupported node existence state")
				}

				n, err = io.ReadFull(r, b[:4]) // level
				if err != nil {
					return errors.Wrapf(err, "read node level")
				}
				read += n
				node.level = int(binary.LittleEndian.Uint32(b[:4]))

				n, err = io.ReadFull(r, b[:4]) // connections count
				if err != nil {
					return errors.Wrapf(err, "read node connections count")
				}
				read += n
				connCount := int(binary.LittleEndian.Uint32(b[:4]))

				if connCount > 0 {
					node.connections = make([][]uint64, connCount)

					for l := 0; l < connCount; l++ {
						n, err = io.ReadFull(r, b[:4]) // connections count at level
						if err != nil {
							return errors.Wrapf(err, "read node connections count at level")
						}
						read += n
						connCountAtLevel := int(binary.LittleEndian.Uint32(b[:4]))

						if connCountAtLevel > 0 {
							node.connections[l] = make([]uint64, connCountAtLevel)

							for c := 0; c < connCountAtLevel; c++ {
								n, err = io.ReadFull(r, b[:8]) // connection at level
								if err != nil {
									return errors.Wrapf(err, "read node connection at level")
								}
								node.connections[l][c] = binary.LittleEndian.Uint64(b[:8])
								read += n
							}
						}

					}
				}

				res.Nodes[currNodeID] = node
				currNodeID++
			}

			return nil
		})
	}

	err = eg.Wait()
	if err != nil {
		return nil, err
	}

	return res, nil
}

type Checkpoint struct {
	NodeID uint64
	Offset uint64
}

func writeCheckpoints(fileName string, checkpoints []Checkpoint) error {
	checkpointFile, err := os.OpenFile(fileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o666)
	if err != nil {
		return fmt.Errorf("open new checkpoint file for writing: %w", err)
	}
	defer checkpointFile.Close()

	// 0-4: checksum
	// 4+: checkpoints (16 bytes each)
	buffer := make([]byte, 4+len(checkpoints)*16)
	offset := 4

	for _, cp := range checkpoints {
		binary.LittleEndian.PutUint64(buffer[offset:offset+8], cp.NodeID)
		offset += 8
		binary.LittleEndian.PutUint64(buffer[offset:offset+8], cp.Offset)
		offset += 8
	}

	checksum := crc32.ChecksumIEEE(buffer[4:])
	binary.LittleEndian.PutUint32(buffer[:4], checksum)

	_, err = checkpointFile.Write(buffer)
	if err != nil {
		return fmt.Errorf("write checkpoint file: %w", err)
	}

	return checkpointFile.Sync()
}

func readCheckpoints(snapshotFileName string) (checkpoints []Checkpoint, err error) {
	cpfn := snapshotFileName + ".checkpoints"

	cpFile, err := os.Open(cpfn)
	if err != nil {
		return nil, err
	}
	defer cpFile.Close()

	buf, err := io.ReadAll(cpFile)
	if err != nil {
		return nil, err
	}
	if len(buf) < 4 {
		return nil, fmt.Errorf("corrupted checkpoint file %q", cpfn)
	}

	checksum := binary.LittleEndian.Uint32(buf[:4])
	actualChecksum := crc32.ChecksumIEEE(buf[4:])
	if checksum != actualChecksum {
		return nil, fmt.Errorf("corrupted checkpoint file %q, checksum mismatch", cpfn)
	}

	checkpoints = make([]Checkpoint, 0, len(buf[4:])/16)
	for i := 4; i < len(buf); i += 16 {
		id := binary.LittleEndian.Uint64(buf[i : i+8])
		offset := binary.LittleEndian.Uint64(buf[i+8 : i+16])
		checkpoints = append(checkpoints, Checkpoint{NodeID: id, Offset: offset})
	}

	return checkpoints, nil
}
