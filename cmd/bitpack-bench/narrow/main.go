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

// narrow converts raw binary matrices dumped by h5dump from 8-byte to
// 4-byte element types, for datasets stored as f64/i64 in HDF5
// (e.g. sphere-1M-meta-dpr). Usage:
//
//	narrow -kind=f64 in.bin out.bin   # float64 -> float32
//	narrow -kind=i64 in.bin out.bin   # int64   -> int32 (must fit)
package main

import (
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
)

func main() {
	kind := flag.String("kind", "f64", "input element type: f64 or i64")
	flag.Parse()
	if flag.NArg() != 2 {
		fmt.Fprintln(os.Stderr, "usage: narrow -kind=f64|i64 <in> <out>")
		os.Exit(1)
	}
	if err := run(*kind, flag.Arg(0), flag.Arg(1)); err != nil {
		fmt.Fprintf(os.Stderr, "narrow: %v\n", err)
		os.Exit(1)
	}
}

func run(kind, inPath, outPath string) error {
	in, err := os.Open(inPath)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(outPath)
	if err != nil {
		return err
	}
	defer out.Close()

	r := bufio.NewReaderSize(in, 1<<20)
	w := bufio.NewWriterSize(out, 1<<20)
	var buf [8]byte
	var o [4]byte
	for {
		if _, err := io.ReadFull(r, buf[:]); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		v := binary.LittleEndian.Uint64(buf[:])
		switch kind {
		case "f64":
			binary.LittleEndian.PutUint32(o[:], math.Float32bits(float32(math.Float64frombits(v))))
		case "i64":
			iv := int64(v)
			if iv > math.MaxInt32 || iv < math.MinInt32 {
				return fmt.Errorf("value %d does not fit int32", iv)
			}
			binary.LittleEndian.PutUint32(o[:], uint32(int32(iv)))
		default:
			return fmt.Errorf("unknown kind %q", kind)
		}
		if _, err := w.Write(o[:]); err != nil {
			return err
		}
	}
	return w.Flush()
}
