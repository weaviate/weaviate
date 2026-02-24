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
	"strconv"
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

const (
	PAXRecordSplitFileOffsetPartName = "WEAVIATE.fileOffset"
)

type SplitFile struct {
	AbsPath        string
	RelPath        string
	FileInfo       fs.FileInfo
	AlreadyWritten int64
}

type compressor interface {
	Flush() error
	Write(p []byte) (n int, err error)
	Close() error
}

type zip struct {
	sourcePath          string
	w                   *tar.Writer
	compressorWriter    compressor
	pipeWriter          *io.PipeWriter
	maxChunkSizeInBytes int64
	splitFileSizeBytes  int64
}

func NewZip(sourcePath string, level int, chunkTargetSize, splitFileSize int64) (zip, entBackup.ReadCloserWithError, error) {
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
	chunkTargetSizeInBytes := chunkTargetSize
	if chunkTargetSizeInBytes == 0 {
		chunkTargetSizeInBytes = int64(1<<63 - 1) // effectively no limit
	}
	if splitFileSize == 0 {
		splitFileSize = int64(1<<63 - 1) // effectively no limit
	}

	return zip{
		sourcePath:          sourcePath,
		compressorWriter:    gzw,
		w:                   tarW,
		pipeWriter:          pw,
		maxChunkSizeInBytes: chunkTargetSizeInBytes,
		splitFileSizeBytes:  splitFileSize,
	}, reader, nil
}

func (z *zip) Close() error {
	return z.CloseWithError(nil)
}

// CloseWithError closes the zip and signals the given error to the consumer.
// If err is non-nil, the consumer's read will return this error instead of EOF.
func (z *zip) CloseWithError(err error) error {
	var err1, err2, err3 error
	err1 = z.w.Close()
	if z.compressorWriter != nil {
		err2 = z.compressorWriter.Close()
	}
	if closeErr := z.pipeWriter.CloseWithError(err); closeErr != nil && !errors.Is(closeErr, io.ErrClosedPipe) {
		err3 = closeErr
	}
	if err1 != nil || err2 != nil || err3 != nil {
		return fmt.Errorf("tar: %w, gzip: %w, pw: %w", err1, err2, err3)
	}
	return nil
}

// WriteShard writes shard internal files including in memory files stored in sd
func (z *zip) WriteShard(ctx context.Context, sd *entBackup.ShardDescriptor, filesInShard *entBackup.FileList, firstChunkForShard bool, preCompressionSize *atomic.Int64) (int64, *SplitFile, error) {
	var written int64

	// write in-memory files only for the first chunk of the shard, these files are small and we can assume that they will
	// always fit into the first chunk
	if firstChunkForShard {
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
				return written, nil, err
			}
			info := vFileInfo{
				name: filepath.Base(x.relPath),
				size: len(x.data),
			}
			preCompressionSize.Add(int64(len(x.data)))
			n, err := z.writeOne(ctx, info, x.relPath, bytes.NewReader(x.data))
			if err != nil {
				return written, nil, err
			}
			written += n
		}
	}

	n, sizeExceededInfo, err := z.WriteRegulars(ctx, filesInShard, preCompressionSize)
	written += n

	return written, sizeExceededInfo, err
}

func (z *zip) WriteRegulars(ctx context.Context, filesInShard *entBackup.FileList, preCompressionSize *atomic.Int64) (int64, *SplitFile, error) {
	// Process files in filesInShard and remove them as we go (pop from front).
	firstFile := true
	written := int64(0)
	for filesInShard.Len() > 0 {
		relPath := filesInShard.Peek()
		if filepath.Base(relPath) == ".DS_Store" {
			filesInShard.PopFront()
			continue
		}
		if err := ctx.Err(); err != nil {
			return written, nil, err
		}
		n, sizeExceededInfo, err := z.WriteRegular(ctx, relPath, preCompressionSize, firstFile)
		if err != nil {
			return written, nil, err
		}
		if sizeExceededInfo != nil {
			// The file was not written because the current chunk is full.
			if sizeExceededInfo.FileInfo == nil {
				// File is below split threshold, it will be written whole on the next chunk.
				return written, nil, nil
			}
			// File exceeds split threshold, it will be split across chunks.
			// Pop it from the list since the split mechanism now owns it.
			filesInShard.PopFront()
			return written, sizeExceededInfo, nil
		}
		// remove processed element from slice
		filesInShard.PopFront()
		written += n
		firstFile = false
	}
	return written, nil, nil
}

func (z *zip) WriteRegular(ctx context.Context, relPath string, preCompressionSize *atomic.Int64, firstFile bool) (written int64, sizeExceeded *SplitFile, err error) {
	if err := ctx.Err(); err != nil {
		return written, nil, err
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
				return written, nil, fmt.Errorf("stat for deleted files: %w", err)
			}
		} else {
			return written, nil, fmt.Errorf("stat: %w", err)
		}
	}
	if !info.Mode().IsRegular() {
		return 0, nil, nil // ignore directories
	}
	// Check if the file exceeds the chunk size
	if preCompressionSize.Load()+info.Size() > z.maxChunkSizeInBytes {
		if info.Size() > z.splitFileSizeBytes {
			// file is larger than the split threshold, split it across chunks
			// (a split part counts as "at least one file" in the chunk)
			return 0, &SplitFile{AbsPath: absPath, RelPath: relPath, FileInfo: info, AlreadyWritten: 0}, nil
		}
		if !firstFile {
			// file doesn't need splitting, but chunk is full - defer to next chunk
			return 0, &SplitFile{}, nil
		}
		// firstFile and below split threshold: write it whole so the chunk is not empty
	}

	f, err := os.Open(absPath)
	if err != nil {
		return written, nil, fmt.Errorf("open: %w", err)
	}
	defer f.Close()

	preCompressionSize.Add(info.Size())

	written, err = z.writeOne(ctx, info, relPath, f)
	return written, nil, err
}

func (z *zip) writeOne(ctx context.Context, info fs.FileInfo, relPath string, r io.Reader) (written int64, err error) {
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

func (z *zip) WriteSplitFile(ctx context.Context, splitFile *SplitFile, preCompressionSize *atomic.Int64) (*SplitFile, error) {
	amountToWrite := min(splitFile.FileInfo.Size()-splitFile.AlreadyWritten, z.splitFileSizeBytes)

	header, err := tar.FileInfoHeader(splitFile.FileInfo, splitFile.FileInfo.Name())
	if err != nil {
		return nil, fmt.Errorf("file header: %w", err)
	}
	header.Name = splitFile.RelPath
	header.ChangeTime = splitFile.FileInfo.ModTime()
	header.PAXRecords = map[string]string{
		PAXRecordSplitFileOffsetPartName: strconv.FormatInt(splitFile.AlreadyWritten, 10),
	}
	header.Size = amountToWrite
	if err := z.w.WriteHeader(header); err != nil {
		return nil, fmt.Errorf("write header: %w", err)
	}

	f, err := os.Open(splitFile.AbsPath)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	defer f.Close()

	if _, err := f.Seek(splitFile.AlreadyWritten, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek to offset %d in file %s: %w", splitFile.AlreadyWritten, splitFile.RelPath, err)
	}

	if _, err := io.CopyN(z.w, f, amountToWrite); err != nil {
		return nil, fmt.Errorf("copy %d bytes from file %s: %w", amountToWrite, splitFile.RelPath, err)
	}
	splitFile.AlreadyWritten += amountToWrite
	preCompressionSize.Add(amountToWrite)

	if splitFile.AlreadyWritten < splitFile.FileInfo.Size() {
		return splitFile, nil
	}
	return nil, nil
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

func (u *unzip) ReadChunk() (written int64, err error) {
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
	part, isSplitFile := h.PAXRecords[PAXRecordSplitFileOffsetPartName]
	if isSplitFile {
		startOffset, err := strconv.ParseInt(part, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid split file part %q: %w", part, err)
		}

		// open without truncating so out-of-order chunks can be written at their offsets
		f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY, os.FileMode(h.Mode))
		if err != nil {
			return 0, fmt.Errorf("open: %w", err)
		}
		defer f.Close()

		// seek to the chunk start
		if _, err := f.Seek(startOffset, io.SeekStart); err != nil {
			return 0, fmt.Errorf("seek: %w", err)
		}

		// write exactly the number of bytes this tar entry contains
		n, err := io.CopyN(f, r, h.Size)
		if err != nil && (!errors.Is(err, io.EOF) || n <= 0) {
			return n, fmt.Errorf("copy split: %w", err)
		}
		return n, nil
	} else {
		f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY, os.FileMode(h.Mode))
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
	src *io.PipeReader
	n   int64
}

func (r *readCloser) Read(p []byte) (n int, err error) {
	n, err = r.src.Read(p)
	atomic.AddInt64(&r.n, int64(n))
	return n, err
}

func (r *readCloser) Close() error { return r.src.Close() }

// CloseWithError closes the reader and signals the given error to the producer.
// If err is non-nil, the producer's write will return this error instead of
// the generic "io: read/write on closed pipe".
func (r *readCloser) CloseWithError(err error) error { return r.src.CloseWithError(err) }

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
