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

package backup

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/weaviate/weaviate/entities/backup"
)

// CompressionLevel represents supported compression level
type CompressionLevel int

const (
	DefaultCompression CompressionLevel = iota
	BestSpeed
	BestCompression
)

type zip struct {
	sourcePath string
	w          *tar.Writer
	gzw        *gzip.Writer
	pipeWriter *io.PipeWriter
	counter    func() int64
}

func NewZip(sourcePath string, level int) (zip, io.ReadCloser) {
	pr, pw := io.Pipe()
	gzw, _ := gzip.NewWriterLevel(pw, zipLevel(level))
	reader := &readCloser{src: pr, n: 0}

	return zip{
		sourcePath: sourcePath,
		gzw:        gzw,
		w:          tar.NewWriter(gzw),
		pipeWriter: pw,
		counter:    reader.counter(),
	}, reader
}

func (z *zip) Close() error {
	var err1, err2, err3 error
	err1 = z.w.Close()
	err2 = z.gzw.Close()
	if err := z.pipeWriter.Close(); err != nil && err != io.ErrClosedPipe {
		err3 = err
	}
	if err1 != nil || err2 != nil || err3 != nil {
		return fmt.Errorf("tar: %w, gzip: %w, pw: %w", err1, err2, err3)
	}
	return nil
}

// WriteShard writes shard internal files including in memory files stored in sd
func (z *zip) WriteShard(ctx context.Context, sd *backup.ShardDescriptor) (written int64, err error) {
	var n int64 // temporary written bytes
	for _, x := range [3]struct {
		relPath string
		data    []byte
		modTime time.Time
	}{
		{relPath: sd.DocIDCounterPath, data: sd.DocIDCounter},
		{relPath: sd.PropLengthTrackerPath, data: sd.PropLengthTracker},
		{relPath: sd.ShardVersionPath, data: sd.Version},
	} {
		info := vFileInfo{
			name: filepath.Base(x.relPath),
			size: len(x.data),
		}
		if n, err = z.writeOne(info, x.relPath, bytes.NewReader(x.data)); err != nil {
			return written, err
		}
		written += n

	}

	n, err = z.WriteRegulars(ctx, sd.Files)
	written += n

	return
}

func (z *zip) WriteRegulars(ctx context.Context, relPaths []string) (written int64, err error) {
	for _, relPath := range relPaths {
		if filepath.Base(relPath) == ".DS_Store" {
			continue
		}
		if err := ctx.Err(); err != nil {
			return written, err
		}
		n, err := z.WriteRegular(relPath)
		if err != nil {
			return written, err
		}
		written += n
	}
	return written, nil
}

func (z *zip) WriteRegular(relPath string) (written int64, err error) {
	// open file for read
	absPath := filepath.Join(z.sourcePath, relPath)
	info, err := os.Stat(absPath)
	if err != nil {
		return written, fmt.Errorf("stat: %w", err)
	}
	if !info.Mode().IsRegular() {
		return 0, nil // ignore directories
	}
	f, err := os.Open(absPath)
	if err != nil {
		return written, fmt.Errorf("open: %w", err)
	}
	defer f.Close()

	return z.writeOne(info, relPath, f)
}

func (z *zip) writeOne(info fs.FileInfo, relPath string, r io.Reader) (written int64, err error) {
	// write info header
	header, err := tar.FileInfoHeader(info, info.Name())
	if err != nil {
		return written, fmt.Errorf("file header: %w", err)
	}
	header.Name = relPath
	header.ChangeTime = info.ModTime()
	if err := z.w.WriteHeader(header); err != nil {
		return written, fmt.Errorf("write header %s: %w", relPath, err)
	}
	// write bytes
	written, err = io.Copy(z.w, r)
	if err != nil {
		return written, fmt.Errorf("copy: %s %w", relPath, err)
	}
	return
}

// lastWritten number of bytes
func (z *zip) lastWritten() int64 {
	return z.counter()
}

type unzip struct {
	destPath   string
	gzr        *gzip.Reader
	r          *tar.Reader
	pipeReader *io.PipeReader
}

func NewUnzip(dst string) (unzip, io.WriteCloser) {
	pr, pw := io.Pipe()
	return unzip{
		destPath:   dst,
		pipeReader: pr,
	}, pw
}

func (u *unzip) init() error {
	if u.gzr != nil {
		return nil
	}
	gz, err := gzip.NewReader(u.pipeReader)
	if err != nil {
		return fmt.Errorf("gzip.NewReader: %w", err)
	}
	u.gzr = gz
	u.r = tar.NewReader(gz)
	return nil
}

func (u *unzip) Close() (err error) {
	var err1, err2 error
	if err := u.pipeReader.Close(); err != nil && err != io.ErrClosedPipe {
		err1 = err
	}
	if u.gzr != nil {
		err2 = u.gzr.Close()
	}
	if err1 != nil || err2 != nil {
		return fmt.Errorf("close pr: %w, gunzip: %w", err1, err2)
	}

	return nil
}

func (u *unzip) ReadChunk() (written int64, err error) {
	if err := u.init(); err != nil {
		return 0, err
	}
	parentPath := ""
	for {
		header, err := u.r.Next()
		if err != nil {
			if err == io.EOF { // end of the loop
				return written, nil
			}
			return written, fmt.Errorf("fetch next: %w", err)
		}
		if header == nil {
			continue
		}

		// target file
		target := filepath.Join(u.destPath, header.Name)
		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o755); err != nil {
				return written, fmt.Errorf("crateDir %s: %w", target, err)
			}
		case tar.TypeReg:
			if pp := filepath.Dir(target); pp != parentPath {
				parentPath = pp
				if err := os.MkdirAll(parentPath, 0o755); err != nil {
					return written, fmt.Errorf("crateDir %s: %w", target, err)
				}
			}
			n, err := copyFile(target, header, u.r)
			if err != nil {
				return written, fmt.Errorf("copy file %s: %w", target, err)
			}
			written += n
		}
	}
}

func copyFile(target string, h *tar.Header, r io.Reader) (written int64, err error) {
	f, err := os.OpenFile(target, os.O_CREATE|os.O_RDWR, os.FileMode(h.Mode))
	if err != nil {
		return written, fmt.Errorf("create: %w", err)
	}
	defer f.Close()
	written, err = io.Copy(f, r)
	if err != nil {
		return written, fmt.Errorf("copy: %w", err)
	}
	return written, nil
}

type vFileInfo struct {
	name    string
	size    int
	modTime time.Time // TODO: get it when parsing source files
}

func (v vFileInfo) Name() string       { return v.name }
func (v vFileInfo) Size() int64        { return int64(v.size) }
func (v vFileInfo) Mode() os.FileMode  { return 0o644 }
func (v vFileInfo) ModTime() time.Time { return v.modTime }
func (v vFileInfo) IsDir() bool        { return false }
func (v vFileInfo) Sys() interface{}   { return nil }

type readCloser struct {
	src io.ReadCloser
	n   int64
}

func (r *readCloser) Read(p []byte) (n int, err error) {
	n, err = r.src.Read(p)
	atomic.AddInt64(&r.n, int64(n))
	return
}

func (r *readCloser) Close() error { return r.src.Close() }

func (r *readCloser) counter() func() int64 {
	return func() int64 {
		return atomic.LoadInt64(&r.n)
	}
}

func zipLevel(level int) int {
	if level < 0 || level > 3 {
		return gzip.DefaultCompression
	}
	switch CompressionLevel(level) {
	case BestSpeed:
		return gzip.BestSpeed
	case BestCompression:
		return gzip.BestCompression
	default:
		return gzip.DefaultCompression
	}
}

type zipConfig struct {
	Level      int
	GoPoolSize int
	ChunkSize  int
}

func newZipConfig(c Compression) zipConfig {
	// convert from MB to byte because input already
	// in MB and validated against min:2 max:512
	switch c.ChunkSize = c.ChunkSize * 1024 * 1024; {
	case c.ChunkSize == 0:
		c.ChunkSize = DefaultChunkSize
	case c.ChunkSize > maxChunkSize:
		c.ChunkSize = maxChunkSize
	case c.ChunkSize < minChunkSize:
		c.ChunkSize = minChunkSize
	}

	return zipConfig{
		Level:      int(c.Level),
		GoPoolSize: routinePoolSize(c.CPUPercentage),
		ChunkSize:  c.ChunkSize,
	}
}
