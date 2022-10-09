//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
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
	ID            string                     `json:"id"`      // User created backup id
	Backend       string                     `json:"backend"` // object store: s3, gcs, ..
	Nodes         map[string]*NodeDescriptor `json:"nodes"`
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

// ShardDescriptor contains everything needed to completely restore a partition of a specific class
type ShardDescriptor struct {
	Name  string   `json:"name"`
	Node  string   `json:"node"`
	Files []string `json:"files"`

	DocIDCounterPath      string `json:"docIdCounterPath"`
	DocIDCounter          []byte `json:"docIdCounter"`
	PropLengthTrackerPath string `json:"propLengthTrackerPath"`
	PropLengthTracker     []byte `json:"propLengthTracker"`
	ShardVersionPath      string `json:"shardVersionPath"`
	Version               []byte `json:"version"`
}

// ClassDescriptor contains everything needed to completely restore a class
type ClassDescriptor struct {
	Name          string            `json:"name"` // DB class name, also selected by user
	Shards        []ShardDescriptor `json:"shards"`
	ShardingState []byte            `json:"shardingState"`
	Schema        []byte            `json:"schema"`
	Error         error             `json:"-"`
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

// Validate validates d
func (d *BackupDescriptor) Validate() error {
	if d.StartedAt.IsZero() || d.ID == "" ||
		d.Version == "" || d.ServerVersion == "" || d.Error != "" {
		return fmt.Errorf("attribute mismatch: [id versions time error]")
	}
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
