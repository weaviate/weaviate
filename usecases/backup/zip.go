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

package backup

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/zstd"
	entBackup "github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/diskio"
)

// CompressionLevel represents supported compression level
type CompressionLevel int

const (
	GzipDefaultCompression CompressionLevel = iota
	GzipBestSpeed
	GzipBestCompression
	ZstdBestSpeed
	ZstdDefaultCompression
	ZstdBestCompression
	NoCompression
)

type compressor interface {
	Flush() error
	Write(p []byte) (n int, err error)
	Close() error
}

type zip struct {
	sourcePath       string
	w                *tar.Writer
	compressorWriter compressor
	pipeWriter       *io.PipeWriter
}

func NewZip(sourcePath string, level int) (zip, io.ReadCloser, error) {
	pr, pw := io.Pipe()
	reader := &readCloser{src: pr, n: 0}

	var gzw compressor
	var tarW *tar.Writer

	switch CompressionLevel(level) {
	case NoCompression:
		// produce raw tar stream without compression
		tarW = tar.NewWriter(pw)
	case ZstdBestSpeed, ZstdDefaultCompression, ZstdBestCompression:
		var zstdLevel zstd.EncoderLevel
		switch CompressionLevel(level) {
		case ZstdBestSpeed:
			zstdLevel = zstd.SpeedFastest
		case ZstdDefaultCompression:
			zstdLevel = zstd.SpeedDefault
		case ZstdBestCompression:
			zstdLevel = zstd.SpeedBetterCompression
		default: // makes linter happy
			return zip{}, nil, fmt.Errorf("unknown zstd compression level %v", level)
		}
		gzw, _ = zstd.NewWriter(pw, zstd.WithEncoderLevel(zstdLevel))
		tarW = tar.NewWriter(gzw)
	case GzipDefaultCompression, GzipBestSpeed, GzipBestCompression:
		gzw, _ = gzip.NewWriterLevel(pw, zipLevel(level))
		tarW = tar.NewWriter(gzw)
	default:
		return zip{}, nil, fmt.Errorf("unknown compression level %v", level)
	}

	return zip{
		sourcePath:       sourcePath,
		compressorWriter: gzw,
		w:                tarW,
		pipeWriter:       pw,
	}, reader, nil
}

func (z *zip) Close() error {
	var err1, err2, err3 error
	err1 = z.w.Close()
	if z.compressorWriter != nil {
		err2 = z.compressorWriter.Close()
	}
	if err := z.pipeWriter.Close(); err != nil && !errors.Is(err, io.ErrClosedPipe) {
		err3 = err
	}
	if err1 != nil || err2 != nil || err3 != nil {
		return fmt.Errorf("tar: %w, gzip: %w, pw: %w", err1, err2, err3)
	}
	return nil
}

// WriteShard writes shard internal files including in memory files stored in sd
func (z *zip) WriteShard(ctx context.Context, sd *entBackup.ShardDescriptor) (written int64, err error) {
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
		if err := ctx.Err(); err != nil {
			return written, err
		}
		info := vFileInfo{
			name: filepath.Base(x.relPath),
			size: len(x.data),
		}
		if n, err = z.writeOne(ctx, info, x.relPath, sd.Name, bytes.NewReader(x.data)); err != nil {
			return written, err
		}
		written += n

	}

	n, err = z.WriteRegulars(ctx, sd.Files, sd.Name)
	written += n

	return written, err
}

func (z *zip) WriteRegulars(ctx context.Context, relPaths []string, shardName string) (written int64, err error) {
	for _, relPath := range relPaths {
		if filepath.Base(relPath) == ".DS_Store" {
			continue
		}
		if err := ctx.Err(); err != nil {
			return written, err
		}
		n, err := z.WriteRegular(ctx, relPath, shardName)
		if err != nil {
			return written, err
		}
		written += n
	}
	return written, nil
}

func (z *zip) WriteRegular(ctx context.Context, relPath, shardName string) (written int64, err error) {
	if err := ctx.Err(); err != nil {
		return written, err
	}
	// open file for read
	absPath := filepath.Join(z.sourcePath, relPath)
	// check if file exists, if not check if the collection has been deleted and is now available with the delete marker
	info, err := os.Stat(absPath)
	if err != nil {
		if os.IsNotExist(err) {
			absPath = filepath.Join(z.sourcePath, entBackup.DeleteMarkerAdd(relPath))
			info, err = os.Stat(absPath)
			if err != nil {
				return written, fmt.Errorf("stat for deleted files: %w", err)
			}
		} else {
			return written, fmt.Errorf("stat: %w", err)
		}
	}
	if !info.Mode().IsRegular() {
		return 0, nil // ignore directories
	}
	f, err := os.Open(absPath)
	if err != nil {
		return written, fmt.Errorf("open: %w", err)
	}
	defer f.Close()

	return z.writeOne(ctx, info, relPath, shardName, f)
}

func (z *zip) writeOne(ctx context.Context, info fs.FileInfo, relPath, shardName string, r io.Reader) (written int64, err error) {
	if err := ctx.Err(); err != nil {
		return written, err
	}
	// write info header
	header, err := tar.FileInfoHeader(info, info.Name())
	if err != nil {
		return written, fmt.Errorf("file header: %w", err)
	}
	header.Name = relPath
	header.ChangeTime = info.ModTime()
	header.Gname = shardName
	if err := z.w.WriteHeader(header); err != nil {
		if errors.Is(ctx.Err(), context.Canceled) {
			// we ignore in case the ctx was cancelled
			return written, nil
		}
		return written, fmt.Errorf("write backup header in file %s: %s: %w", z.sourcePath, relPath, err)
	}
	// write bytes
	written, err = io.Copy(z.w, r)
	if err != nil {
		if errors.Is(ctx.Err(), context.Canceled) {
			// we ignore in case the ctx was cancelled
			return written, nil
		}
		return written, fmt.Errorf("copy: %s %w", relPath, err)
	}
	return written, err
}

type zstdWrapper struct {
	z *zstd.Decoder
}

func (z zstdWrapper) Read(p []byte) (n int, err error) {
	return z.z.Read(p)
}

func (z zstdWrapper) Close() error {
	z.z.Close()
	return nil
}

type unzip struct {
	destPath        string
	gzr             io.ReadCloser
	r               *tar.Reader
	pipeReader      *io.PipeReader
	compressionType entBackup.CompressionType
}

func NewUnzip(dst string, compressionType entBackup.CompressionType) (unzip, io.WriteCloser) {
	pr, pw := io.Pipe()
	return unzip{
		destPath:        dst,
		pipeReader:      pr,
		compressionType: compressionType,
	}, pw
}

func (u *unzip) init() error {
	if u.gzr != nil {
		return nil
	}
	var dec io.ReadCloser
	var err error
	switch u.compressionType {
	case entBackup.CompressionNone:
		u.r = tar.NewReader(u.pipeReader)
		return nil
	case entBackup.CompressionZSTD:
		zstdDec, err := zstd.NewReader(u.pipeReader)
		if err != nil {
			return fmt.Errorf("zstd.NewReader: %w", err)
		}
		dec = zstdWrapper{z: zstdDec}
	case entBackup.CompressionGZIP:
		dec, err = gzip.NewReader(u.pipeReader)
		if err != nil {
			return fmt.Errorf("gzip.NewReader: %w", err)
		}
	}
	u.gzr = dec
	u.r = tar.NewReader(dec)
	return nil
}

func (u *unzip) Close() (err error) {
	var err1, err2 error
	if err := u.pipeReader.Close(); err != nil && !errors.Is(err, io.ErrClosedPipe) {
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

func (u *unzip) ReadChunk(shardsToWrite []string) (written int64, err error) {
	if err := u.init(); err != nil {
		return 0, err
	}
	parentPath := ""
	for {
		header, err := u.r.Next()
		if err != nil {
			if errors.Is(err, io.EOF) { // end of the loop
				return written, nil
			}
			return written, fmt.Errorf("fetch next: %w", err)
		}
		if header == nil {
			continue
		}
		shardName := header.Gname
		if shardsToWrite != nil && !slices.Contains(shardsToWrite, shardName) {
			// skip this shard
			continue
		}

		// target file
		target, err := diskio.SanitizeFilePathJoin(u.destPath, header.Name)
		if err != nil {
			return written, fmt.Errorf("sanitize file path %s: %w", header.Name, err)
		}
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
	return n, err
}

func (r *readCloser) Close() error { return r.src.Close() }

func zipLevel(level int) int {
	if level < 0 || level > 3 {
		return gzip.DefaultCompression
	}
	switch CompressionLevel(level) {
	case GzipBestSpeed:
		return gzip.BestSpeed
	case GzipBestCompression:
		return gzip.BestCompression
	default:
		return gzip.DefaultCompression
	}
}

type zipConfig struct {
	Level      int
	GoPoolSize int
}

func newZipConfig(c Compression) zipConfig {
	return zipConfig{
		Level:      int(c.Level),
		GoPoolSize: routinePoolSize(c.CPUPercentage),
	}
}
