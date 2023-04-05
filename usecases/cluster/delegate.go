//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package cluster

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/sirupsen/logrus"
)

// _OpCode represents the type of supported operation
type _OpCode uint8

const (
	// _ProtoVersion internal protocol version for exchanging messages
	_ProtoVersion uint8 = 1
	// _OpCodeDisk operation code for getting disk space
	_OpCodeDisk _OpCode = 1
	// _ProtoTTL used to decide when to update the cache
	_ProtoTTL = time.Second * 3
)

// spaceMsg is used to notify other nodes about current disk usage
type spaceMsg struct {
	header
	DiskUsage
	Node string // node space
}

// header of an operation
type header struct {
	// OpCode operation code
	OpCode _OpCode
	// ProtoVersion protocol we will speak
	ProtoVersion uint8
}

// DiskUsage contains total and available space in B
type DiskUsage struct {
	// Total disk space
	Total uint64
	// Total available space
	Available uint64
}

// NodeInfo disk space
type NodeInfo struct {
	DiskUsage
	LastTimeMilli int64 // last update time in milli seconds
}

func (d *spaceMsg) marshal() (data []byte, err error) {
	buf := bytes.NewBuffer(make([]byte, 0, 20+len(d.Node)))
	if err := binary.Write(buf, binary.BigEndian, d.header); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, d.DiskUsage); err != nil {
		return nil, err
	}
	_, err = buf.Write([]byte(d.Node))
	return buf.Bytes(), err
}

func (d *spaceMsg) unmarshal(data []byte) error {
	rd := bytes.NewReader(data)
	if err := binary.Read(rd, binary.BigEndian, &d.header); err != nil {
		return err
	}
	if err := binary.Read(rd, binary.BigEndian, &d.DiskUsage); err != nil {
		return err
	}
	d.Node = string(data[len(data)-rd.Len():])
	return nil
}

// delegate implements the memberList delegate interface
type delegate struct {
	Name      string
	dataPath  string
	diskUsage func(path string) (DiskUsage, error)
	log       logrus.FieldLogger
	sync.Mutex
	Cache map[string]NodeInfo
}

// init must be called first to initialize the cache
func (d *delegate) init() error {
	d.Cache = make(map[string]NodeInfo, 32)
	d.diskUsage = diskSpace
	space, err := diskSpace(d.dataPath)
	if err != nil {
		return fmt.Errorf("disk_space: %w", err)
	}

	d.set(d.Name, NodeInfo{space, time.Now().UnixMilli()}) // cache
	return nil
}

// NodeMeta is used to retrieve meta-data about the current node
// when broadcasting an alive message. It's length is limited to
// the given byte size. This metadata is available in the Node structure.
func (d *delegate) NodeMeta(limit int) (meta []byte) {
	return nil
}

// LocalState is used for a TCP Push/Pull. This is sent to
// the remote side in addition to the membership information. Any
// data can be sent here. See MergeRemoteState as well. The `join`
// boolean indicates this is for a join instead of a push/pull.
func (d *delegate) LocalState(join bool) []byte {
	prv, _ := d.get(d.Name) // value from cache
	var (
		info NodeInfo = prv
		err  error
	)
	// renew cached value if ttl expires
	if prv.Available == 0 || time.Since(time.UnixMilli(prv.LastTimeMilli)) >= _ProtoTTL {
		info.DiskUsage, err = d.diskUsage(d.dataPath)
		if err != nil {
			d.log.WithField("action", "delegate.local_state.disk_usage").Error(err)
			return nil
		}
		info.LastTimeMilli = time.Now().UnixMilli()
	}

	if prv.LastTimeMilli != info.LastTimeMilli {
		d.set(d.Name, info) // cache new value
	}

	x := spaceMsg{
		header{OpCode: _OpCodeDisk, ProtoVersion: _ProtoVersion},
		info.DiskUsage,
		d.Name,
	}
	bytes, err := x.marshal()
	if err != nil {
		d.log.WithField("action", "delegate.local_state.marshal").Error(err)
		return nil
	}
	return bytes
}

// MergeRemoteState is invoked after a TCP Push/Pull. This is the
// state received from the remote side and is the result of the
// remote side's LocalState call. The 'join'
// boolean indicates this is for a join instead of a push/pull.
func (d *delegate) MergeRemoteState(data []byte, join bool) {
	var x spaceMsg
	if err := x.unmarshal(data); err != nil || x.Node == "" {
		d.log.WithField("action", "delegate.merge_remote.unmarshal").
			WithField("data", string(data)).Error(err)
		return
	}
	info := NodeInfo{x.DiskUsage, time.Now().UnixMilli()}
	d.set(x.Node, info)
}

func (d *delegate) NotifyMsg(data []byte) {}

func (d *delegate) GetBroadcasts(overhead, limit int) [][]byte {
	return nil
}

// get returns info about about a specific node in the cluster
func (d *delegate) get(node string) (NodeInfo, bool) {
	d.Lock()
	defer d.Unlock()
	x, ok := d.Cache[node]
	return x, ok
}

func (d *delegate) set(node string, x NodeInfo) {
	d.Lock()
	defer d.Unlock()
	d.Cache[node] = x
}

// delete key from the cache
func (d *delegate) delete(node string) {
	d.Lock()
	defer d.Unlock()
	delete(d.Cache, node)
}

// sortCandidates by the amount of free space in descending order
//
// Two nodes are considered equivalent if the difference between their
// free spaces is less than 4KB.
// The free space is just an rough estimate of the actual amount.
// The Lower bound 4KB helps to mitigate the risk of selecting same set of nodes
// when selections happens concurrently on different initiator nodes.
func (d *delegate) sortCandidates(names []string) []string {
	d.Lock()
	defer d.Unlock()
	m := d.Cache
	sort.Slice(names, func(i, j int) bool {
		return (m[names[j]].Available >> 12) < (m[names[i]].Available >> 12)
	})
	return names
}

// events implement memberlist.EventDelegate interface
// EventDelegate is a simpler delegate that is used only to receive
// notifications about members joining and leaving. The methods in this
// delegate may be called by multiple goroutines, but never concurrently.
// This allows you to reason about ordering.
type events struct {
	d *delegate
}

// NotifyJoin is invoked when a node is detected to have joined.
// The Node argument must not be modified.
func (e events) NotifyJoin(*memberlist.Node) {}

// NotifyLeave is invoked when a node is detected to have left.
// The Node argument must not be modified.
func (e events) NotifyLeave(node *memberlist.Node) {
	e.d.delete(node.Name)
}

// NotifyUpdate is invoked when a node is detected to have
// updated, usually involving the meta data. The Node argument
// must not be modified.
func (e events) NotifyUpdate(*memberlist.Node) {}

// diskSpace return the disk space usage
func diskSpace(path string) (DiskUsage, error) {
	fs := syscall.Statfs_t{}
	err := syscall.Statfs(path, &fs)
	if err != nil {
		return DiskUsage{}, err
	}
	return DiskUsage{
		Total:     fs.Blocks * uint64(fs.Bsize),
		Available: fs.Bavail * uint64(fs.Bsize),
	}, nil
}
