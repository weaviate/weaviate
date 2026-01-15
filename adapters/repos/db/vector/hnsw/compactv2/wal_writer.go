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

package compactv2

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/multivector"
	"github.com/weaviate/weaviate/entities/errorcompounder"
)

// WALWriter writes HNSW commits to an io.Writer.
// It handles the low-level serialization of commit types and their data.
type WALWriter struct {
	w io.Writer
}

// NewWALWriter creates a new WAL writer.
func NewWALWriter(w io.Writer) *WALWriter {
	return &WALWriter{w: w}
}

// WriteAddNode writes an AddNode commit.
func (w *WALWriter) WriteAddNode(id uint64, level uint16) error {
	ec := errorcompounder.New()
	ec.Add(writeCommitType(w.w, AddNode))
	ec.Add(writeUint64(w.w, id))
	ec.Add(writeUint16(w.w, level))
	return ec.ToError()
}

// WriteDeleteNode writes a DeleteNode commit.
func (w *WALWriter) WriteDeleteNode(id uint64) error {
	ec := errorcompounder.New()
	ec.Add(writeCommitType(w.w, DeleteNode))
	ec.Add(writeUint64(w.w, id))
	return ec.ToError()
}

// WriteSetEntryPointMaxLevel writes a SetEntryPointMaxLevel commit.
func (w *WALWriter) WriteSetEntryPointMaxLevel(entrypoint uint64, level uint16) error {
	ec := errorcompounder.New()
	ec.Add(writeCommitType(w.w, SetEntryPointMaxLevel))
	ec.Add(writeUint64(w.w, entrypoint))
	ec.Add(writeUint16(w.w, level))
	return ec.ToError()
}

// WriteAddLinkAtLevel writes an AddLinkAtLevel commit.
func (w *WALWriter) WriteAddLinkAtLevel(source uint64, level uint16, target uint64) error {
	ec := errorcompounder.New()
	ec.Add(writeCommitType(w.w, AddLinkAtLevel))
	ec.Add(writeUint64(w.w, source))
	ec.Add(writeUint16(w.w, level))
	ec.Add(writeUint64(w.w, target))
	return ec.ToError()
}

// WriteAddLinksAtLevel writes an AddLinksAtLevel commit.
func (w *WALWriter) WriteAddLinksAtLevel(source uint64, level uint16, targets []uint64) error {
	toWrite := make([]byte, 13+len(targets)*8)
	toWrite[0] = byte(AddLinksAtLevel)
	binary.LittleEndian.PutUint64(toWrite[1:9], source)
	binary.LittleEndian.PutUint16(toWrite[9:11], level)
	binary.LittleEndian.PutUint16(toWrite[11:13], uint16(len(targets)))
	for i, target := range targets {
		offsetStart := 13 + i*8
		offsetEnd := offsetStart + 8
		binary.LittleEndian.PutUint64(toWrite[offsetStart:offsetEnd], target)
	}
	_, err := w.w.Write(toWrite)
	return err
}

// WriteReplaceLinksAtLevel writes a ReplaceLinksAtLevel commit.
func (w *WALWriter) WriteReplaceLinksAtLevel(source uint64, level uint16, targets []uint64) error {
	ec := errorcompounder.New()
	ec.Add(writeCommitType(w.w, ReplaceLinksAtLevel))
	ec.Add(writeUint64(w.w, source))
	ec.Add(writeUint16(w.w, level))

	targetLength := len(targets)
	if targetLength > math.MaxUint16 {
		targetLength = math.MaxUint16
	}
	ec.Add(writeUint16(w.w, uint16(targetLength)))
	ec.Add(writeUint64Slice(w.w, targets[:targetLength]))

	return ec.ToError()
}

// WriteClearLinks writes a ClearLinks commit.
func (w *WALWriter) WriteClearLinks(id uint64) error {
	ec := errorcompounder.New()
	ec.Add(writeCommitType(w.w, ClearLinks))
	ec.Add(writeUint64(w.w, id))
	return ec.ToError()
}

// WriteClearLinksAtLevel writes a ClearLinksAtLevel commit.
func (w *WALWriter) WriteClearLinksAtLevel(id uint64, level uint16) error {
	ec := errorcompounder.New()
	ec.Add(writeCommitType(w.w, ClearLinksAtLevel))
	ec.Add(writeUint64(w.w, id))
	ec.Add(writeUint16(w.w, level))
	return ec.ToError()
}

// WriteAddTombstone writes an AddTombstone commit.
func (w *WALWriter) WriteAddTombstone(id uint64) error {
	ec := errorcompounder.New()
	ec.Add(writeCommitType(w.w, AddTombstone))
	ec.Add(writeUint64(w.w, id))
	return ec.ToError()
}

// WriteRemoveTombstone writes a RemoveTombstone commit.
func (w *WALWriter) WriteRemoveTombstone(id uint64) error {
	ec := errorcompounder.New()
	ec.Add(writeCommitType(w.w, RemoveTombstone))
	ec.Add(writeUint64(w.w, id))
	return ec.ToError()
}

// WriteResetIndex writes a ResetIndex commit.
func (w *WALWriter) WriteResetIndex() error {
	return writeCommitType(w.w, ResetIndex)
}

// WriteAddPQ writes an AddPQ commit.
func (w *WALWriter) WriteAddPQ(data *compressionhelpers.PQData) error {
	toWrite := make([]byte, 10)
	toWrite[0] = byte(AddPQ)
	binary.LittleEndian.PutUint16(toWrite[1:3], data.Dimensions)
	toWrite[3] = byte(data.EncoderType)
	binary.LittleEndian.PutUint16(toWrite[4:6], data.Ks)
	binary.LittleEndian.PutUint16(toWrite[6:8], data.M)
	toWrite[8] = data.EncoderDistribution
	if data.UseBitsEncoding {
		toWrite[9] = 1
	} else {
		toWrite[9] = 0
	}

	for _, encoder := range data.Encoders {
		toWrite = append(toWrite, encoder.ExposeDataForRestore()...)
	}
	_, err := w.w.Write(toWrite)
	return err
}

// WriteAddSQ writes an AddSQ commit.
func (w *WALWriter) WriteAddSQ(data *compressionhelpers.SQData) error {
	toWrite := make([]byte, 11)
	toWrite[0] = byte(AddSQ)
	binary.LittleEndian.PutUint32(toWrite[1:], math.Float32bits(data.A))
	binary.LittleEndian.PutUint32(toWrite[5:], math.Float32bits(data.B))
	binary.LittleEndian.PutUint16(toWrite[9:], data.Dimensions)
	_, err := w.w.Write(toWrite)
	return err
}

// WriteAddRQ writes an AddRQ commit.
func (w *WALWriter) WriteAddRQ(data *compressionhelpers.RQData) error {
	swapSize := 2 * data.Rotation.Rounds * (data.Rotation.OutputDim / 2) * 2
	signSize := 4 * data.Rotation.Rounds * data.Rotation.OutputDim
	var buf bytes.Buffer
	buf.Grow(17 + int(swapSize) + int(signSize))

	buf.WriteByte(byte(AddRQ))
	binary.Write(&buf, binary.LittleEndian, data.InputDim)
	binary.Write(&buf, binary.LittleEndian, data.Bits)
	binary.Write(&buf, binary.LittleEndian, data.Rotation.OutputDim)
	binary.Write(&buf, binary.LittleEndian, data.Rotation.Rounds)

	for _, swap := range data.Rotation.Swaps {
		for _, dim := range swap {
			binary.Write(&buf, binary.LittleEndian, dim.I)
			binary.Write(&buf, binary.LittleEndian, dim.J)
		}
	}

	for _, sign := range data.Rotation.Signs {
		for _, dim := range sign {
			binary.Write(&buf, binary.LittleEndian, dim)
		}
	}

	_, err := w.w.Write(buf.Bytes())
	return err
}

// WriteAddBRQ writes an AddBRQ commit.
func (w *WALWriter) WriteAddBRQ(data *compressionhelpers.BRQData) error {
	swapSize := 2 * data.Rotation.Rounds * (data.Rotation.OutputDim / 2) * 2
	signSize := 4 * data.Rotation.Rounds * data.Rotation.OutputDim
	roundingSize := 4 * data.Rotation.OutputDim
	var buf bytes.Buffer
	buf.Grow(13 + int(swapSize) + int(signSize) + int(roundingSize))

	buf.WriteByte(byte(AddBRQ))
	binary.Write(&buf, binary.LittleEndian, data.InputDim)
	binary.Write(&buf, binary.LittleEndian, data.Rotation.OutputDim)
	binary.Write(&buf, binary.LittleEndian, data.Rotation.Rounds)

	for _, swap := range data.Rotation.Swaps {
		for _, dim := range swap {
			binary.Write(&buf, binary.LittleEndian, dim.I)
			binary.Write(&buf, binary.LittleEndian, dim.J)
		}
	}

	for _, sign := range data.Rotation.Signs {
		for _, dim := range sign {
			binary.Write(&buf, binary.LittleEndian, dim)
		}
	}

	for _, rounding := range data.Rounding {
		binary.Write(&buf, binary.LittleEndian, rounding)
	}

	_, err := w.w.Write(buf.Bytes())
	return err
}

// WriteAddMuvera writes an AddMuvera commit.
func (w *WALWriter) WriteAddMuvera(data *multivector.MuveraData) error {
	gSize := 4 * data.Repetitions * data.KSim * data.Dimensions
	dSize := 4 * data.Repetitions * data.DProjections * data.Dimensions
	var buf bytes.Buffer
	buf.Grow(21 + int(gSize) + int(dSize))

	buf.WriteByte(byte(AddMuvera))
	binary.Write(&buf, binary.LittleEndian, data.KSim)
	binary.Write(&buf, binary.LittleEndian, data.NumClusters)
	binary.Write(&buf, binary.LittleEndian, data.Dimensions)
	binary.Write(&buf, binary.LittleEndian, data.DProjections)
	binary.Write(&buf, binary.LittleEndian, data.Repetitions)

	for _, gaussian := range data.Gaussians {
		for _, cluster := range gaussian {
			for _, el := range cluster {
				binary.Write(&buf, binary.LittleEndian, math.Float32bits(el))
			}
		}
	}

	for _, matrix := range data.S {
		for _, vector := range matrix {
			for _, el := range vector {
				binary.Write(&buf, binary.LittleEndian, math.Float32bits(el))
			}
		}
	}

	_, err := w.w.Write(buf.Bytes())
	return err
}

// Helper functions for writing primitive types

func writeCommitType(w io.Writer, ct HnswCommitType) error {
	var b [1]byte
	b[0] = byte(ct)
	_, err := w.Write(b[:])
	return err
}

func writeUint64(w io.Writer, v uint64) error {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], v)
	_, err := w.Write(b[:])
	return err
}

func writeUint32(w io.Writer, v uint32) error {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], v)
	_, err := w.Write(b[:])
	return err
}

func writeUint16(w io.Writer, v uint16) error {
	var b [2]byte
	binary.LittleEndian.PutUint16(b[:], v)
	_, err := w.Write(b[:])
	return err
}

func writeFloat32(w io.Writer, v float32) error {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], math.Float32bits(v))
	_, err := w.Write(b[:])
	return err
}

func writeByte(w io.Writer, v byte) error {
	var b [1]byte
	b[0] = v
	_, err := w.Write(b[:])
	return err
}

func writeBool(w io.Writer, v bool) error {
	var b [1]byte
	if v {
		b[0] = 1
	}
	_, err := w.Write(b[:])
	return err
}

func writeUint64Slice(w io.Writer, slice []uint64) error {
	for _, v := range slice {
		if err := writeUint64(w, v); err != nil {
			return errors.Wrap(err, "write uint64 slice element")
		}
	}
	return nil
}
