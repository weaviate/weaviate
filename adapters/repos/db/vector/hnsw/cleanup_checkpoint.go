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

package hnsw

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
)

const (
	// cleanupCheckpointVersion is the current checkpoint format version.
	// Increment this when the format changes incompatibly.
	cleanupCheckpointVersion uint32 = 1

	// cleanupCheckpointMagic is a magic number to identify checkpoint files.
	cleanupCheckpointMagic uint32 = 0x48435043 // "HCPC" = HNSW Cleanup Checkpoint
)

// CleanupCheckpoint represents the persistent state of an in-progress
// tombstone cleanup generation. It allows cleanup to resume after
// interruption without restarting from the beginning.
type CleanupCheckpoint struct {
	// Version is the checkpoint format version for compatibility checks.
	Version uint32

	// GenerationID uniquely identifies this cleanup generation.
	// Monotonically increasing counter.
	GenerationID uint64

	// TombstoneIDs is the exact set of tombstones being cleaned in this
	// generation. This is a sorted copy of the deleteList created at
	// generation start. It must be preserved exactly to ensure correctness:
	// tombstones may only be removed if the entire graph was scanned against
	// this exact set.
	TombstoneIDs []uint64

	// TotalNodes is the graph size (len(h.nodes)) at generation start.
	// Used for progress calculation.
	TotalNodes uint64

	// Watermark is the highest node ID where all IDs <= Watermark have been
	// processed. On resume, scanning starts from Watermark + 1.
	Watermark uint64

	// StartedAt is when this generation started.
	StartedAt time.Time

	// UpdatedAt is when this checkpoint was last persisted.
	UpdatedAt time.Time

	// Checksum is CRC32 of all fields above for corruption detection.
	Checksum uint32
}

// computeChecksum calculates a CRC32 checksum of the checkpoint data.
func (cp *CleanupCheckpoint) computeChecksum() uint32 {
	h := crc32.NewIEEE()

	// Write fixed-size fields
	binary.Write(h, binary.LittleEndian, cp.Version)
	binary.Write(h, binary.LittleEndian, cp.GenerationID)
	binary.Write(h, binary.LittleEndian, uint64(len(cp.TombstoneIDs)))
	for _, id := range cp.TombstoneIDs {
		binary.Write(h, binary.LittleEndian, id)
	}
	binary.Write(h, binary.LittleEndian, cp.TotalNodes)
	binary.Write(h, binary.LittleEndian, cp.Watermark)
	binary.Write(h, binary.LittleEndian, cp.StartedAt.UnixNano())
	binary.Write(h, binary.LittleEndian, cp.UpdatedAt.UnixNano())

	return h.Sum32()
}

// encode serializes the checkpoint to a byte slice.
func (cp *CleanupCheckpoint) encode() []byte {
	// Calculate size:
	// 4 (magic) + 4 (version) + 8 (gen) + 8 (tombstone count) +
	// 8*len(tombstones) + 8 (total) + 8 (watermark) + 8 (started) +
	// 8 (updated) + 4 (checksum)
	size := 4 + 4 + 8 + 8 + 8*len(cp.TombstoneIDs) + 8 + 8 + 8 + 8 + 4
	buf := make([]byte, size)
	offset := 0

	// Magic number
	binary.LittleEndian.PutUint32(buf[offset:], cleanupCheckpointMagic)
	offset += 4

	// Version
	binary.LittleEndian.PutUint32(buf[offset:], cp.Version)
	offset += 4

	// GenerationID
	binary.LittleEndian.PutUint64(buf[offset:], cp.GenerationID)
	offset += 8

	// TombstoneIDs length and data
	binary.LittleEndian.PutUint64(buf[offset:], uint64(len(cp.TombstoneIDs)))
	offset += 8
	for _, id := range cp.TombstoneIDs {
		binary.LittleEndian.PutUint64(buf[offset:], id)
		offset += 8
	}

	// TotalNodes
	binary.LittleEndian.PutUint64(buf[offset:], cp.TotalNodes)
	offset += 8

	// Watermark
	binary.LittleEndian.PutUint64(buf[offset:], cp.Watermark)
	offset += 8

	// StartedAt
	binary.LittleEndian.PutUint64(buf[offset:], uint64(cp.StartedAt.UnixNano()))
	offset += 8

	// UpdatedAt
	binary.LittleEndian.PutUint64(buf[offset:], uint64(cp.UpdatedAt.UnixNano()))
	offset += 8

	// Compute and write checksum
	cp.Checksum = cp.computeChecksum()
	binary.LittleEndian.PutUint32(buf[offset:], cp.Checksum)

	return buf
}

// decodeCheckpoint deserializes a checkpoint from a byte slice.
func decodeCheckpoint(data []byte) (*CleanupCheckpoint, error) {
	if len(data) < 4+4+8+8+8+8+8+8+4 {
		return nil, fmt.Errorf("checkpoint data too short: %d bytes", len(data))
	}

	offset := 0

	// Magic number
	magic := binary.LittleEndian.Uint32(data[offset:])
	if magic != cleanupCheckpointMagic {
		return nil, fmt.Errorf("invalid checkpoint magic: %x", magic)
	}
	offset += 4

	cp := &CleanupCheckpoint{}

	// Version
	cp.Version = binary.LittleEndian.Uint32(data[offset:])
	if cp.Version != cleanupCheckpointVersion {
		return nil, fmt.Errorf("unsupported checkpoint version: %d (expected %d)",
			cp.Version, cleanupCheckpointVersion)
	}
	offset += 4

	// GenerationID
	cp.GenerationID = binary.LittleEndian.Uint64(data[offset:])
	offset += 8

	// TombstoneIDs
	tombstoneCount := binary.LittleEndian.Uint64(data[offset:])
	offset += 8

	expectedSize := offset + int(tombstoneCount)*8 + 8 + 8 + 8 + 8 + 4
	if len(data) < expectedSize {
		return nil, fmt.Errorf("checkpoint data truncated: expected %d bytes, got %d",
			expectedSize, len(data))
	}

	cp.TombstoneIDs = make([]uint64, tombstoneCount)
	for i := uint64(0); i < tombstoneCount; i++ {
		cp.TombstoneIDs[i] = binary.LittleEndian.Uint64(data[offset:])
		offset += 8
	}

	// TotalNodes
	cp.TotalNodes = binary.LittleEndian.Uint64(data[offset:])
	offset += 8

	// Watermark
	cp.Watermark = binary.LittleEndian.Uint64(data[offset:])
	offset += 8

	// StartedAt
	startedNano := binary.LittleEndian.Uint64(data[offset:])
	cp.StartedAt = time.Unix(0, int64(startedNano))
	offset += 8

	// UpdatedAt
	updatedNano := binary.LittleEndian.Uint64(data[offset:])
	cp.UpdatedAt = time.Unix(0, int64(updatedNano))
	offset += 8

	// Checksum
	storedChecksum := binary.LittleEndian.Uint32(data[offset:])
	cp.Checksum = storedChecksum

	// Verify checksum
	expectedChecksum := cp.computeChecksum()
	if storedChecksum != expectedChecksum {
		return nil, fmt.Errorf("checkpoint checksum mismatch: stored %x, computed %x",
			storedChecksum, expectedChecksum)
	}

	return cp, nil
}

// saveCleanupCheckpoint atomically writes a checkpoint to disk.
// It uses a temp file + rename pattern for atomicity.
func saveCleanupCheckpoint(path string, cp *CleanupCheckpoint, fs common.FS) error {
	cp.UpdatedAt = time.Now()
	data := cp.encode()

	tempPath := path + ".tmp"

	// Write to temp file using Create + Write
	f, err := fs.Create(tempPath)
	if err != nil {
		return fmt.Errorf("create temp checkpoint: %w", err)
	}
	_, writeErr := f.Write(data)
	closeErr := f.Close()
	if writeErr != nil {
		fs.Remove(tempPath)
		return fmt.Errorf("write temp checkpoint: %w", writeErr)
	}
	if closeErr != nil {
		fs.Remove(tempPath)
		return fmt.Errorf("close temp checkpoint: %w", closeErr)
	}

	// Atomic rename
	if err := fs.Rename(tempPath, path); err != nil {
		// Try to clean up temp file
		fs.Remove(tempPath)
		return fmt.Errorf("rename checkpoint: %w", err)
	}

	return nil
}

// loadCleanupCheckpoint reads and validates a checkpoint from disk.
// Returns (nil, nil) if the checkpoint file doesn't exist.
// Returns (nil, error) if the checkpoint is corrupt or unreadable.
func loadCleanupCheckpoint(path string, fs common.FS) (*CleanupCheckpoint, error) {
	f, err := fs.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // No checkpoint exists
		}
		return nil, fmt.Errorf("open checkpoint file: %w", err)
	}
	defer f.Close()

	data, err := io.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("read checkpoint file: %w", err)
	}

	cp, err := decodeCheckpoint(data)
	if err != nil {
		return nil, fmt.Errorf("decode checkpoint: %w", err)
	}

	return cp, nil
}

// deleteCleanupCheckpoint removes the checkpoint file.
func deleteCleanupCheckpoint(path string, fs common.FS) error {
	err := fs.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove checkpoint: %w", err)
	}
	return nil
}

// cleanupCheckpointPath returns the path for the cleanup checkpoint file.
func cleanupCheckpointPath(rootPath, indexID string) string {
	return fmt.Sprintf("%s/%s.hnsw.cleanup.checkpoint", rootPath, indexID)
}

// progressTracker tracks cleanup progress across concurrent workers.
// It provides safe watermark computation that accounts for out-of-order
// completion of work items.
type progressTracker struct {
	mu sync.Mutex

	// nextID is the next node ID to be claimed.
	nextID uint64

	// totalNodes is the upper bound (len(h.nodes) at start).
	totalNodes uint64

	// inFlight tracks IDs currently being processed.
	// An ID is added to inFlight atomically when claimed,
	// and removed when completed.
	inFlight map[uint64]struct{}

	// lastPersistedWatermark is the watermark from the last checkpoint persist.
	lastPersistedWatermark uint64
}

// newProgressTracker creates a progress tracker for a cleanup operation.
// startFrom is the first node ID to process (watermark + 1 on resume, 0 on fresh start).
// totalNodes is the graph size.
func newProgressTracker(startFrom, totalNodes uint64) *progressTracker {
	watermark := uint64(0)
	if startFrom > 0 {
		watermark = startFrom - 1
	}
	return &progressTracker{
		nextID:                 startFrom,
		totalNodes:             totalNodes,
		inFlight:               make(map[uint64]struct{}),
		lastPersistedWatermark: watermark,
	}
}

// claim atomically claims the next node ID for processing.
// Returns (id, true) if there's work available, or (0, false) if all nodes
// have been claimed.
//
// IMPORTANT: This method atomically increments the counter AND adds the ID
// to the in-flight set. This prevents a race where an ID could be "in limbo"
// between claiming and tracking.
func (p *progressTracker) claim() (uint64, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.nextID >= p.totalNodes {
		return 0, false
	}

	id := p.nextID
	p.nextID++
	p.inFlight[id] = struct{}{}
	return id, true
}

// complete marks a node ID as finished processing.
func (p *progressTracker) complete(id uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.inFlight, id)
}

// getSafeWatermark returns the highest node ID where all IDs <= watermark
// are guaranteed to be complete.
//
// Correctness argument:
// - All IDs < nextID have been claimed (nextID only increases under lock)
// - All IDs in inFlight are being processed (added atomically at claim)
// - All IDs < nextID AND not in inFlight are complete
// - Therefore, min(inFlight) - 1 is the highest contiguous completed ID
func (p *progressTracker) getSafeWatermark() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.inFlight) == 0 {
		// All claimed IDs are complete
		if p.nextID == 0 {
			return 0
		}
		return p.nextID - 1
	}

	// Find minimum in-flight ID
	minInFlight := uint64(math.MaxUint64)
	for id := range p.inFlight {
		if id < minInFlight {
			minInFlight = id
		}
	}

	if minInFlight == 0 {
		return 0 // Can't have watermark below 0
	}
	return minInFlight - 1
}

// getProgress returns current progress statistics.
func (p *progressTracker) getProgress() (claimed, total, inFlight uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.nextID, p.totalNodes, uint64(len(p.inFlight))
}

// shouldPersist returns true if the watermark has advanced significantly
// since the last persist.
func (p *progressTracker) shouldPersist(minAdvance uint64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	watermark := p.getSafeWatermarkLocked()
	return watermark >= p.lastPersistedWatermark+minAdvance
}

// getSafeWatermarkLocked is the lock-free version for internal use.
func (p *progressTracker) getSafeWatermarkLocked() uint64 {
	if len(p.inFlight) == 0 {
		if p.nextID == 0 {
			return 0
		}
		return p.nextID - 1
	}

	minInFlight := uint64(math.MaxUint64)
	for id := range p.inFlight {
		if id < minInFlight {
			minInFlight = id
		}
	}

	if minInFlight == 0 {
		return 0
	}
	return minInFlight - 1
}

// markPersisted updates the last persisted watermark.
func (p *progressTracker) markPersisted(watermark uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.lastPersistedWatermark = watermark
}

// createCheckpointFromDeleteList creates a new checkpoint for a fresh
// cleanup generation.
func createCheckpointFromDeleteList(
	generationID uint64,
	deleteList helpers.AllowList,
	totalNodes uint64,
) *CleanupCheckpoint {
	// Get sorted tombstone IDs from the allow list
	tombstoneIDs := deleteList.Slice()
	sort.Slice(tombstoneIDs, func(i, j int) bool {
		return tombstoneIDs[i] < tombstoneIDs[j]
	})

	return &CleanupCheckpoint{
		Version:      cleanupCheckpointVersion,
		GenerationID: generationID,
		TombstoneIDs: tombstoneIDs,
		TotalNodes:   totalNodes,
		Watermark:    0,
		StartedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}
}

// rebuildDeleteListFromCheckpoint reconstructs the deleteList AllowList
// from checkpoint tombstone IDs.
func rebuildDeleteListFromCheckpoint(cp *CleanupCheckpoint) helpers.AllowList {
	return helpers.NewAllowList(cp.TombstoneIDs...)
}
