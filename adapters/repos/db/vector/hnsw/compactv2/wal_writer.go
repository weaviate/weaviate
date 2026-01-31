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

package compactv2

import (
	"encoding/binary"
	"io"
	"math"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/multivector"
	"github.com/weaviate/weaviate/entities/errorcompounder"
	"github.com/weaviate/weaviate/entities/vectorindex/compression"
	"github.com/weaviate/weaviate/usecases/byteops"
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
func (w *WALWriter) WriteAddPQ(data *compression.PQData) error {
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
func (w *WALWriter) WriteAddSQ(data *compression.SQData) error {
	toWrite := make([]byte, 11)
	toWrite[0] = byte(AddSQ)
	binary.LittleEndian.PutUint32(toWrite[1:], math.Float32bits(data.A))
	binary.LittleEndian.PutUint32(toWrite[5:], math.Float32bits(data.B))
	binary.LittleEndian.PutUint16(toWrite[9:], data.Dimensions)
	_, err := w.w.Write(toWrite)
	return err
}

// WriteAddRQ writes an AddRQ commit.
func (w *WALWriter) WriteAddRQ(data *compression.RQData) error {
	// Calculate sizes: header (1 + 4 + 4 + 4 + 4 = 17 bytes)
	// Swaps: rounds * (outputDim/2) * 4 bytes (2 uint16 per swap)
	// Signs: rounds * outputDim * 4 bytes (1 float32 per sign)
	swapSize := int(data.Rotation.Rounds * (data.Rotation.OutputDim / 2) * 4)
	signSize := int(data.Rotation.Rounds * data.Rotation.OutputDim * 4)
	totalSize := 17 + swapSize + signSize

	buf := make([]byte, totalSize)
	rw := byteops.NewReadWriter(buf)

	rw.WriteByte(byte(AddRQ))
	rw.WriteUint32(data.InputDim)
	rw.WriteUint32(data.Bits)
	rw.WriteUint32(data.Rotation.OutputDim)
	rw.WriteUint32(data.Rotation.Rounds)

	for _, swap := range data.Rotation.Swaps {
		for _, dim := range swap {
			rw.WriteUint16(dim.I)
			rw.WriteUint16(dim.J)
		}
	}

	for _, sign := range data.Rotation.Signs {
		_ = rw.CopyBytesToBuffer(byteops.Fp32SliceToBytes(sign))
	}

	_, err := w.w.Write(buf)
	return err
}

// WriteAddBRQ writes an AddBRQ commit.
func (w *WALWriter) WriteAddBRQ(data *compression.BRQData) error {
	// Calculate sizes: header (1 + 4 + 4 + 4 = 13 bytes)
	// Swaps: rounds * (outputDim/2) * 4 bytes (2 uint16 per swap)
	// Signs: rounds * outputDim * 4 bytes (1 float32 per sign)
	// Rounding: outputDim * 4 bytes (1 float32 per rounding)
	swapSize := int(data.Rotation.Rounds * (data.Rotation.OutputDim / 2) * 4)
	signSize := int(data.Rotation.Rounds * data.Rotation.OutputDim * 4)
	roundingSize := int(data.Rotation.OutputDim * 4)
	totalSize := 13 + swapSize + signSize + roundingSize

	buf := make([]byte, totalSize)
	rw := byteops.NewReadWriter(buf)

	rw.WriteByte(byte(AddBRQ))
	rw.WriteUint32(data.InputDim)
	rw.WriteUint32(data.Rotation.OutputDim)
	rw.WriteUint32(data.Rotation.Rounds)

	for _, swap := range data.Rotation.Swaps {
		for _, dim := range swap {
			rw.WriteUint16(dim.I)
			rw.WriteUint16(dim.J)
		}
	}

	for _, sign := range data.Rotation.Signs {
		_ = rw.CopyBytesToBuffer(byteops.Fp32SliceToBytes(sign))
	}

	_ = rw.CopyBytesToBuffer(byteops.Fp32SliceToBytes(data.Rounding))

	_, err := w.w.Write(buf)
	return err
}

// WriteAddMuvera writes an AddMuvera commit.
func (w *WALWriter) WriteAddMuvera(data *multivector.MuveraData) error {
	// Calculate sizes: header (1 + 4 + 4 + 4 + 4 + 4 = 21 bytes)
	// Gaussians: repetitions * kSim * dimensions * 4 bytes (float32)
	// S matrices: repetitions * dProjections * dimensions * 4 bytes (float32)
	gSize := int(data.Repetitions * data.KSim * data.Dimensions * 4)
	dSize := int(data.Repetitions * data.DProjections * data.Dimensions * 4)
	totalSize := 21 + gSize + dSize

	buf := make([]byte, totalSize)
	rw := byteops.NewReadWriter(buf)

	rw.WriteByte(byte(AddMuvera))
	rw.WriteUint32(data.KSim)
	rw.WriteUint32(data.NumClusters)
	rw.WriteUint32(data.Dimensions)
	rw.WriteUint32(data.DProjections)
	rw.WriteUint32(data.Repetitions)

	for _, gaussian := range data.Gaussians {
		for _, cluster := range gaussian {
			_ = rw.CopyBytesToBuffer(byteops.Fp32SliceToBytes(cluster))
		}
	}

	for _, matrix := range data.S {
		for _, vector := range matrix {
			_ = rw.CopyBytesToBuffer(byteops.Fp32SliceToBytes(vector))
		}
	}

	_, err := w.w.Write(buf)
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
