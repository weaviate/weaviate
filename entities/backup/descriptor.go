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
	"fmt"
	"os"
	"time"

	"github.com/weaviate/weaviate/entities/diskio"
)

// DeleteMarkerAdd marks folders of indices that have been deleted during an ongoing backup and are already removed from
// store and schema. However, we want to keep them on disk to ensure that the backup can complete. The folders are
// removed after backup completion. In case of a crash they are deleted at the next startup.
const DeleteMarker = "__DELETE_ME_AFTER_BACKUP__"

func DeleteMarkerAdd(filename string) string {
	return fmt.Sprintf("%s%s", DeleteMarker, filename)
}

// NodeDescriptor contains data related to one participant in DBRO
type NodeDescriptor struct {
	Classes                 []string `json:"classes"`
	Status                  Status   `json:"status"`
	Error                   string   `json:"error"`
	PreCompressionSizeBytes int64    `json:"preCompressionSizeBytes"` // Size of this node's backup in bytes before compression
}

// DistributedBAckupDescriptor contains everything need to completely restore a distributed backup
type DistributedBackupDescriptor struct {
	StartedAt               time.Time                  `json:"startedAt"`
	CompletedAt             time.Time                  `json:"completedAt"`
	ID                      string                     `json:"id"` // User created backup id
	Nodes                   map[string]*NodeDescriptor `json:"nodes"`
	NodeMapping             map[string]string          `json:"node_mapping"`
	Status                  Status                     `json:"status"`  //
	Version                 string                     `json:"version"` //
	ServerVersion           string                     `json:"serverVersion"`
	Leader                  string                     `json:"leader"`
	Error                   string                     `json:"error"`
	PreCompressionSizeBytes int64                      `json:"preCompressionSizeBytes"` // Size of this node's backup in bytes before compression
	CompressionType         CompressionType            `json:"compressionType"`
	BaseBackupID            string                     `json:"baseBackupId"`
}

// Len returns how many nodes exist in d
func (d *DistributedBackupDescriptor) Len() int {
	return len(d.Nodes)
}

// Count number of classes
func (d *DistributedBackupDescriptor) Count() int {
	count := 0
	for _, desc := range d.Nodes {
		count += len(desc.Classes)
	}
	return count
}

// RemoveEmpty removes any nodes with an empty class list
func (d *DistributedBackupDescriptor) RemoveEmpty() *DistributedBackupDescriptor {
	for node, desc := range d.Nodes {
		if len(desc.Classes) == 0 {
			delete(d.Nodes, node)
		}
	}
	return d
}

// Classes returns all classes contained in d
func (d *DistributedBackupDescriptor) Classes() []string {
	set := make(map[string]struct{}, 32)
	for _, desc := range d.Nodes {
		for _, cls := range desc.Classes {
			set[cls] = struct{}{}
		}
	}
	lst := make([]string, len(set))
	i := 0
	for cls := range set {
		lst[i] = cls
		i++
	}
	return lst
}

// Filter classes based on predicate
func (d *DistributedBackupDescriptor) Filter(pred func(s string) bool) {
	for _, desc := range d.Nodes {
		cs := make([]string, 0, len(desc.Classes))
		for _, cls := range desc.Classes {
			if pred(cls) {
				cs = append(cs, cls)
			}
		}
		if len(cs) != len(desc.Classes) {
			desc.Classes = cs
		}
	}
}

// Include only these classes and remove everything else
func (d *DistributedBackupDescriptor) Include(classes []string) {
	if len(classes) == 0 {
		return
	}
	set := make(map[string]struct{}, len(classes))
	for _, cls := range classes {
		set[cls] = struct{}{}
	}
	pred := func(s string) bool {
		_, ok := set[s]
		return ok
	}
	d.Filter(pred)
}

// Exclude removes classes from d
func (d *DistributedBackupDescriptor) Exclude(classes []string) {
	if len(classes) == 0 {
		return
	}
	set := make(map[string]struct{}, len(classes))
	for _, cls := range classes {
		set[cls] = struct{}{}
	}
	pred := func(s string) bool {
		_, ok := set[s]
		return !ok
	}
	d.Filter(pred)
}

// ToMappedNodeName will return nodeName after applying d.NodeMapping translation on it.
// If nodeName is not contained in d.nodeMapping, returns nodeName unmodified
func (d *DistributedBackupDescriptor) ToMappedNodeName(nodeName string) string {
	if newNodeName, ok := d.NodeMapping[nodeName]; ok {
		return newNodeName
	}
	return nodeName
}

// ToOriginalNodeName will return nodeName after trying to find an original node name from d.NodeMapping values.
// If nodeName is not contained in d.nodeMapping values, returns nodeName unmodified
func (d *DistributedBackupDescriptor) ToOriginalNodeName(nodeName string) string {
	for oldNodeName, newNodeName := range d.NodeMapping {
		if newNodeName == nodeName {
			return oldNodeName
		}
	}
	return nodeName
}

// ApplyNodeMapping applies d.NodeMapping translation to d.Nodes. If a node in d.Nodes is not translated by d.NodeMapping, it will remain
// unchanged.
func (d *DistributedBackupDescriptor) ApplyNodeMapping() {
	if len(d.NodeMapping) == 0 {
		return
	}

	for k, v := range d.NodeMapping {
		if nodeDescriptor, ok := d.Nodes[k]; !ok {
			d.Nodes[v] = nodeDescriptor
			delete(d.Nodes, k)
		}
	}
}

// AllExist checks if all classes exist in d.
// It returns either "" or the first class which it could not find
func (d *DistributedBackupDescriptor) AllExist(classes []string) string {
	if len(classes) == 0 {
		return ""
	}
	set := make(map[string]struct{}, len(classes))
	for _, cls := range classes {
		set[cls] = struct{}{}
	}
	for _, dest := range d.Nodes {
		for _, cls := range dest.Classes {
			delete(set, cls)
			if len(set) == 0 {
				return ""
			}
		}
	}
	first := ""
	for k := range set {
		first = k
		break
	}
	return first
}

func (d *DistributedBackupDescriptor) Validate() error {
	if d.StartedAt.IsZero() || d.ID == "" ||
		d.Version == "" || d.ServerVersion == "" || d.Error != "" {
		return fmt.Errorf("attribute mismatch: [id versions time error]")
	}
	if len(d.Nodes) == 0 {
		return fmt.Errorf("empty list of node descriptors")
	}
	return nil
}

// resetStatus sets status and sub-statuses to Started
// It also empties error and sub-errors
func (d *DistributedBackupDescriptor) ResetStatus() *DistributedBackupDescriptor {
	d.Status = Started
	d.Error = ""
	d.StartedAt = time.Now()
	d.CompletedAt = time.Time{}
	for _, node := range d.Nodes {
		node.Status = Started
		node.Error = ""
	}
	return d
}

func (d *DistributedBackupDescriptor) GetBaseBackupID() string {
	return d.BaseBackupID
}

func (d *DistributedBackupDescriptor) GetStatus() Status {
	return d.Status
}

func (d *DistributedBackupDescriptor) GetCompressionType() CompressionType {
	return d.CompressionType
}

// ShardDescriptor contains everything needed to completely restore a partition of a specific class
type ShardDescriptor struct {
	Name                  string                 `json:"name"`
	Node                  string                 `json:"node"`
	Files                 []string               `json:"files,omitempty"`
	BigFilesChunk         map[string]BigFileInfo `json:"bigFilesChunk,omitempty"`
	IncrementalBackupInfo IncrementalBackupInfos `json:"incrementalBackupInfo,omitempty"`

	DocIDCounterPath      string `json:"docIdCounterPath,omitempty"`
	DocIDCounter          []byte `json:"docIdCounter,omitempty"`
	PropLengthTrackerPath string `json:"propLengthTrackerPath,omitempty"`
	PropLengthTracker     []byte `json:"propLengthTracker,omitempty"`
	ShardVersionPath      string `json:"shardVersionPath,omitempty"`
	Version               []byte `json:"version,omitempty"`
}

// ClearTemporary clears fields that are no longer needed once compression is done.
// These fields are not required in versions > 1 because they are stored in the tarball.
func (s *ShardDescriptor) ClearTemporary() {
	s.ShardVersionPath = ""
	s.Version = nil

	s.DocIDCounterPath = ""
	s.DocIDCounter = nil

	s.PropLengthTrackerPath = ""
	s.PropLengthTracker = nil
}

func (s *ShardDescriptor) FillFileInfo(files []string, shardBaseDescrs []ShardAndID, rootPath string) error {
	if len(shardBaseDescrs) == 0 {
		s.Files = files
		return nil
	}

FilesLoop:
	for _, file := range files {
		for _, shardBaseDescr := range shardBaseDescrs {
			if info, ok := shardBaseDescr.ShardDesc.BigFilesChunk[file]; ok {
				absPath, err := diskio.SanitizeFilePathJoin(rootPath, file)
				if err != nil {
					return fmt.Errorf("sanitize file path %v: %w", file, err)
				}
				infoNew, err := os.Stat(absPath)
				if err != nil {
					return fmt.Errorf("stat big file %v: %w", file, err)
				}
				// unchanged files. These can be skipped in incremental backup and restored from the previous backup
				if info.Size == infoNew.Size() && info.ModifiedAt.Equal(infoNew.ModTime()) {
					if s.IncrementalBackupInfo.FilesPerBackup == nil {
						s.IncrementalBackupInfo.FilesPerBackup = make(map[string][]IncrementalBackupInfo)
					}
					// files that are skipped due to being unchanged from base backup
					s.IncrementalBackupInfo.FilesPerBackup[shardBaseDescr.BackupID] = append(
						s.IncrementalBackupInfo.FilesPerBackup[shardBaseDescr.BackupID],
						IncrementalBackupInfo{File: file, ChunkKeys: info.ChunkKeys},
					)
					s.IncrementalBackupInfo.TotalSize += info.Size
					s.IncrementalBackupInfo.NumFilesSkipped += 1
					continue FilesLoop
				}
			}
		}

		// files to backup
		s.Files = append(s.Files, file)
	}
	return nil
}

type ShardAndID struct {
	ShardDesc *ShardDescriptor
	BackupID  string
}

// FileList holds a list of file paths and allows modification of the underlying slice
type FileList struct {
	Files     []string
	FileSizes map[string]int64 // map of relative file path to file size in bytes
	// Top100Size is the size of the 100th biggest file (or smallest if fewer than 100 files),
	// with a minimum of 1MB. This can be used for chunk size optimization.
	Top100Size int64
	start      int
}

// Len returns the number of files in the list
func (f *FileList) Len() int {
	if f == nil {
		return 0
	}
	if f.start >= len(f.Files) {
		return 0
	}
	return len(f.Files) - f.start
}

// PopFront removes and returns the first file from the list
func (f *FileList) PopFront() string {
	if f == nil || f.Len() == 0 {
		return ""
	}
	file := f.Files[f.start]
	f.start++
	// When all elements have been popped, release the backing array.
	if f.start >= len(f.Files) {
		f.Files = nil
		f.start = 0
	}
	return file
}

// Peek returns the first file without removing it
func (f *FileList) Peek() string {
	if f == nil || f.Len() == 0 {
		return ""
	}
	return f.Files[f.start]
}

// PeekAt returns the file at offset i from start without removing it.
// Returns "" if i is out of range.
func (f *FileList) PeekAt(i int) string {
	if f == nil || i < 0 || i >= f.Len() {
		return ""
	}
	return f.Files[f.start+i]
}

// RemoveIndices removes files at the given offsets (relative to start) from the list.
// Indices must be sorted ascending. Remaining files preserve their relative order.
func (f *FileList) RemoveIndices(indices []int) {
	if f == nil || len(indices) == 0 {
		return
	}
	n := f.Len()
	remove := make(map[int]struct{}, len(indices))
	for _, idx := range indices {
		if idx >= 0 && idx < n {
			remove[f.start+idx] = struct{}{}
		}
	}
	if len(remove) == 0 {
		return
	}
	kept := make([]string, 0, len(f.Files)-len(remove))
	for i, file := range f.Files {
		if _, ok := remove[i]; !ok {
			kept = append(kept, file)
		}
	}
	// Adjust start: elements before f.start that were not removed stay in place.
	// Since we only remove elements at indices >= f.start, the new start stays at
	// the same position in the kept slice minus any removed elements before it (none).
	f.Files = kept
	// All removed indices are >= f.start, so start stays the same.
}

// GetFileSize returns the pre-collected size for a file, or -1 if not found
func (f *FileList) GetFileSize(relPath string) int64 {
	if f == nil || f.FileSizes == nil {
		return -1
	}
	if size, ok := f.FileSizes[relPath]; ok {
		return size
	}
	return -1
}

type IncrementalBackupInfos struct {
	FilesPerBackup map[string][]IncrementalBackupInfo `json:"filesPerBackup,omitempty"`
	// TotalSize and NumFilesSkipped are not needed for restore, only for correct
	// calculation of sizes during the backup process.
	TotalSize       int64 `json:"-"`
	NumFilesSkipped int   `json:"-"`
}

type IncrementalBackupInfo struct {
	File      string   `json:"file"`
	ChunkKeys []string `json:"chunkKeys"`
}

type BigFileInfo struct {
	ChunkKeys  []string  `json:"chunkKeys"`
	Size       int64     `json:"size"`
	ModifiedAt time.Time `json:"modifiedAt"`
}

// ClassDescriptor contains everything needed to completely restore a class
type ClassDescriptor struct {
	Name          string             `json:"name"` // DB class name, also selected by user
	BackupID      string             `json:"backupId"`
	Shards        []*ShardDescriptor `json:"shards"`
	ShardingState []byte             `json:"shardingState"`
	Schema        []byte             `json:"schema"`
	Aliases       []byte             `json:"aliases"`

	// AliasesIncluded makes the old backup backward compatible when
	// old backups are restored by newer ClassDescriptor that supports
	// aliases
	AliasesIncluded         bool               `json:"aliasesIncluded"`
	Chunks                  map[int32][]string `json:"chunks,omitempty"`
	Error                   error              `json:"-"`
	PreCompressionSizeBytes int64              `json:"preCompressionSizeBytes"` // Size of this class's backup in bytes before compression
}

func (c *ClassDescriptor) GetShardDescriptor(shardName string) *ShardDescriptor {
	for _, shard := range c.Shards {
		if shard.Name == shardName {
			return shard
		}
	}
	return nil
}

type CompressionType string

const (
	CompressionZSTD CompressionType = "zstd"
	CompressionGZIP CompressionType = "gzip"
	CompressionNone CompressionType = "none"
)

// BackupDescriptor contains everything needed to completely restore a list of classes
type BackupDescriptor struct {
	StartedAt               time.Time         `json:"startedAt"`
	CompletedAt             time.Time         `json:"completedAt"`
	ID                      string            `json:"id"` // User created backup id
	Classes                 []ClassDescriptor `json:"classes"`
	RbacBackups             []byte            `json:"rbacBackups"`
	UserBackups             []byte            `json:"userBackups"`
	Status                  Status            `json:"status"`  // "STARTED|TRANSFERRING|TRANSFERRED|SUCCESS|FAILED|CANCELED"
	Version                 string            `json:"version"` //
	ServerVersion           string            `json:"serverVersion"`
	Error                   string            `json:"error"`
	PreCompressionSizeBytes int64             `json:"preCompressionSizeBytes"` // Size of this node's backup in bytes before compression
	CompressionType         *CompressionType  `json:"compressionType,omitempty"`
	BaseBackupID            string            `json:"baseBackupId,omitempty"`
}

func (d *BackupDescriptor) GetCompressionType() CompressionType {
	if d.CompressionType == nil {
		// backward compatibility with old backups that don't have this field and default to gzip
		return CompressionGZIP
	}
	return *d.CompressionType
}

func (d *BackupDescriptor) GetStatus() Status {
	return d.Status
}

// List all existing classes in d
func (d *BackupDescriptor) List() []string {
	lst := make([]string, len(d.Classes))
	for i, cls := range d.Classes {
		lst[i] = cls.Name
	}
	return lst
}

func (d *BackupDescriptor) GetBaseBackupID() string {
	return d.BaseBackupID
}

// AllExist checks if all classes exist in d.
// It returns either "" or the first class which it could not find
func (d *BackupDescriptor) AllExist(classes []string) string {
	if len(classes) == 0 {
		return ""
	}
	set := make(map[string]struct{}, len(classes))
	for _, cls := range classes {
		set[cls] = struct{}{}
	}
	for _, dest := range d.Classes {
		delete(set, dest.Name)
	}
	first := ""
	for k := range set {
		first = k
		break
	}
	return first
}

// Include only these classes and remove everything else
func (d *BackupDescriptor) Include(classes []string) {
	if len(classes) == 0 {
		return
	}
	set := make(map[string]struct{}, len(classes))
	for _, cls := range classes {
		set[cls] = struct{}{}
	}
	pred := func(s string) bool {
		_, ok := set[s]
		return ok
	}
	d.Filter(pred)
}

// Exclude removes classes from d
func (d *BackupDescriptor) Exclude(classes []string) {
	if len(classes) == 0 {
		return
	}
	set := make(map[string]struct{}, len(classes))
	for _, cls := range classes {
		set[cls] = struct{}{}
	}
	pred := func(s string) bool {
		_, ok := set[s]
		return !ok
	}
	d.Filter(pred)
}

// Filter classes based on predicate
func (d *BackupDescriptor) Filter(pred func(s string) bool) {
	cs := make([]ClassDescriptor, 0, len(d.Classes))
	for _, dest := range d.Classes {
		if pred(dest.Name) {
			cs = append(cs, dest)
		}
	}
	d.Classes = cs
}

func (d *BackupDescriptor) GetClassDescriptor(className string) *ClassDescriptor {
	for i := range d.Classes {
		if d.Classes[i].Name == className {
			return &d.Classes[i]
		}
	}
	return nil
}

// ValidateV1 validates d
func (d *BackupDescriptor) validateV1() error {
	for _, c := range d.Classes {
		if c.Name == "" || len(c.Schema) == 0 || len(c.ShardingState) == 0 {
			return fmt.Errorf("invalid class %q: [name schema sharding]", c.Name)
		}
		for _, s := range c.Shards {
			n := len(s.Files)
			if s.Name == "" || s.Node == "" || s.DocIDCounterPath == "" ||
				s.ShardVersionPath == "" || s.PropLengthTrackerPath == "" ||
				(n > 0 && (len(s.DocIDCounter) == 0 ||
					len(s.PropLengthTracker) == 0 ||
					len(s.Version) == 0)) {
				return fmt.Errorf("invalid shard %q.%q", c.Name, s.Name)
			}
			for i, fpath := range s.Files {
				if fpath == "" {
					return fmt.Errorf("invalid shard %q.%q: file number %d", c.Name, s.Name, i)
				}
			}
		}
	}
	return nil
}

func (d *BackupDescriptor) Validate(newSchema bool) error {
	if d.StartedAt.IsZero() || d.ID == "" ||
		d.Version == "" || d.ServerVersion == "" || d.Error != "" {
		return fmt.Errorf("attribute mismatch: [id versions time error]")
	}
	if !newSchema {
		return d.validateV1()
	}
	for _, c := range d.Classes {
		if c.Name == "" || len(c.Schema) == 0 || len(c.ShardingState) == 0 {
			return fmt.Errorf("class=%q: invalid attributes [name schema sharding]", c.Name)
		}
		for _, s := range c.Shards {
			if s.Name == "" || s.Node == "" {
				return fmt.Errorf("class=%q: invalid shard %q node=%q", c.Name, s.Name, s.Node)
			}
		}
	}
	return nil
}

// ToDistributed is used just for backward compatibility with the old version.
func (d *BackupDescriptor) ToDistributed() *DistributedBackupDescriptor {
	node, cs := "", d.List()
	for _, xs := range d.Classes {
		for _, s := range xs.Shards {
			node = s.Node
		}
	}
	result := &DistributedBackupDescriptor{
		StartedAt:               d.StartedAt,
		CompletedAt:             d.CompletedAt,
		ID:                      d.ID,
		Status:                  Status(d.Status),
		Version:                 d.Version,
		ServerVersion:           d.ServerVersion,
		Error:                   d.Error,
		PreCompressionSizeBytes: d.PreCompressionSizeBytes, // Copy pre-compression size
	}
	if node != "" && len(cs) > 0 {
		result.Nodes = map[string]*NodeDescriptor{node: {Classes: cs}}
	}
	return result
}
