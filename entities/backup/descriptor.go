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
	"fmt"
	"time"
)

// NodeDescriptor contains data related to one participant in DBRO
type NodeDescriptor struct {
	Classes []string `json:"classes"`
	Status  Status   `json:"status"`
	Error   string   `json:"error"`
}

// DistributedBAckupDescriptor contains everything need to completely restore a distributed backup
type DistributedBackupDescriptor struct {
	StartedAt     time.Time                  `json:"startedAt"`
	CompletedAt   time.Time                  `json:"completedAt"`
	ID            string                     `json:"id"` // User created backup id
	Nodes         map[string]*NodeDescriptor `json:"nodes"`
	NodeMapping   map[string]string          `json:"node_mapping"`
	Status        Status                     `json:"status"`  //
	Version       string                     `json:"version"` //
	ServerVersion string                     `json:"serverVersion"`
	Error         string                     `json:"error"`
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

// ShardDescriptor contains everything needed to completely restore a partition of a specific class
type ShardDescriptor struct {
	Name  string   `json:"name"`
	Node  string   `json:"node"`
	Files []string `json:"files,omitempty"`

	DocIDCounterPath      string `json:"docIdCounterPath,omitempty"`
	DocIDCounter          []byte `json:"docIdCounter,omitempty"`
	PropLengthTrackerPath string `json:"propLengthTrackerPath,omitempty"`
	PropLengthTracker     []byte `json:"propLengthTracker,omitempty"`
	ShardVersionPath      string `json:"shardVersionPath,omitempty"`
	Version               []byte `json:"version,omitempty"`
	Chunk                 int32  `json:"chunk"`
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

// ClassDescriptor contains everything needed to completely restore a class
type ClassDescriptor struct {
	Name          string             `json:"name"` // DB class name, also selected by user
	Shards        []*ShardDescriptor `json:"shards"`
	ShardingState []byte             `json:"shardingState"`
	Schema        []byte             `json:"schema"`
	Chunks        map[int32][]string `json:"chunks,omitempty"`
	Error         error              `json:"-"`
}

// BackupDescriptor contains everything needed to completely restore a list of classes
type BackupDescriptor struct {
	StartedAt     time.Time         `json:"startedAt"`
	CompletedAt   time.Time         `json:"completedAt"`
	ID            string            `json:"id"` // User created backup id
	Classes       []ClassDescriptor `json:"classes"`
	Status        string            `json:"status"`  // "STARTED|TRANSFERRING|TRANSFERRED|SUCCESS|FAILED"
	Version       string            `json:"version"` //
	ServerVersion string            `json:"serverVersion"`
	Error         string            `json:"error"`
}

// List all existing classes in d
func (d *BackupDescriptor) List() []string {
	lst := make([]string, len(d.Classes))
	for i, cls := range d.Classes {
		lst[i] = cls.Name
	}
	return lst
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
		StartedAt:     d.StartedAt,
		CompletedAt:   d.CompletedAt,
		ID:            d.ID,
		Status:        Status(d.Status),
		Version:       d.Version,
		ServerVersion: d.ServerVersion,
		Error:         d.Error,
	}
	if node != "" && len(cs) > 0 {
		result.Nodes = map[string]*NodeDescriptor{node: {Classes: cs}}
	}
	return result
}
