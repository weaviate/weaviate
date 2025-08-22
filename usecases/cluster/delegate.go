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

package cluster

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/sirupsen/logrus"

	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// _OpCode represents the type of supported operation
type _OpCode uint8

const (
	// _ProtoVersion internal protocol version for exchanging messages
	_ProtoVersion uint8 = 1
	// _OpCodeDisk operation code for getting disk space
	_OpCodeDisk _OpCode = 1
	// _ProtoTTL used to decide when to update the cache
	_ProtoTTL = time.Second * 8
)

type NodeStatus string

const (
	NodeStatusActive   NodeStatus = "active"
	NodeStatusShutdown NodeStatus = "shutdown"
)

// spaceMsg is used to notify other nodes about current disk usage
type spaceMsg struct {
	header
	DiskUsage
	NodeLen uint8  // = len(Node) is required to marshal Node
	Node    string // node space
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
	LastTimeMilli int64      // last update time in milliseconds
	Status        NodeStatus // indicates node status e.g. shutdown (gracefully leaving)
}

func (d *spaceMsg) marshal() (data []byte, err error) {
	buf := bytes.NewBuffer(make([]byte, 0, 24+len(d.Node)))
	if err := binary.Write(buf, binary.BigEndian, d.header); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, d.DiskUsage); err != nil {
		return nil, err
	}
	// code node name starting by its length
	if err := buf.WriteByte(d.NodeLen); err != nil {
		return nil, err
	}
	_, err = buf.Write([]byte(d.Node))
	return buf.Bytes(), err
}

func (d *spaceMsg) unmarshal(data []byte) (err error) {
	rd := bytes.NewReader(data)
	if err = binary.Read(rd, binary.BigEndian, &d.header); err != nil {
		return
	}
	if err = binary.Read(rd, binary.BigEndian, &d.DiskUsage); err != nil {
		return
	}

	// decode node name start by its length
	if d.NodeLen, err = rd.ReadByte(); err != nil {
		return
	}
	begin := len(data) - rd.Len()
	end := begin + int(d.NodeLen)
	// make sure this version is backward compatible
	if _ProtoVersion <= 1 && begin+int(d.NodeLen) != len(data) {
		begin-- // since previous version doesn't encode the length
		end = len(data)
		d.NodeLen = uint8(end - begin)
	}
	d.Node = string(data[begin:end])
	return nil
}

// delegate implements the memberList delegate interface
type delegate struct {
	Name     string
	dataPath string
	log      logrus.FieldLogger
	sync.Mutex
	Cache map[string]NodeInfo

	mutex    sync.Mutex
	hostInfo NodeInfo

	metadata NodeMetadata
}

type NodeMetadata struct {
	RestPort int        `json:"rest_port"`
	GrpcPort int        `json:"grpc_port"`
	Status   NodeStatus `json:"status"`
}

func (d *delegate) setOwnSpace(x DiskUsage) {
	d.mutex.Lock()
	d.hostInfo = NodeInfo{DiskUsage: x, LastTimeMilli: time.Now().UnixMilli(), Status: NodeStatusActive}
	d.mutex.Unlock()
}

func (d *delegate) ownInfo() NodeInfo {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	return d.hostInfo
}

// init must be called first to initialize the cache
func (d *delegate) init(diskSpace func(path string) (DiskUsage, error)) error {
	d.Cache = make(map[string]NodeInfo, 32)
	if diskSpace == nil {
		return fmt.Errorf("function calculating disk space cannot be empty")
	}
	lastTime := time.Now()
	minUpdatePeriod := time.Second + _ProtoTTL/3
	space, err := diskSpace(d.dataPath)
	if err != nil {
		lastTime = lastTime.Add(-minUpdatePeriod)
		d.log.WithError(err).Error("calculate disk space")
	}

	d.setOwnSpace(space)
	d.set(d.Name, NodeInfo{DiskUsage: space, LastTimeMilli: lastTime.UnixMilli(), Status: NodeStatusActive})

	// delegate remains alive throughout the entire program.
	enterrors.GoWrapper(func() { d.updater(_ProtoTTL, minUpdatePeriod, diskSpace) }, d.log)
	return nil
}

// NodeMeta is used to retrieve meta-data about the current node
// when broadcasting an alive message. It's length is limited to
// the given byte size. This metadata is available in the Node structure.
func (d *delegate) NodeMeta(limit int) (meta []byte) {
	data, err := json.Marshal(d.metadata)
	if err != nil {
		return nil
	}
	if len(data) > limit {
		return nil
	}
	return data
}

// LocalState is used for a TCP Push/Pull. This is sent to
// the remote side in addition to the membership information. Any
// data can be sent here. See MergeRemoteState as well. The `join`
// boolean indicates this is for a join instead of a push/pull.
func (d *delegate) LocalState(join bool) []byte {
	var (
		info = d.ownInfo()
		err  error
	)

	d.set(d.Name, info) // cache new value

	x := spaceMsg{
		header{
			OpCode:       _OpCodeDisk,
			ProtoVersion: _ProtoVersion,
		},
		info.DiskUsage,
		uint8(len(d.Name)),
		d.Name,
	}
	bytes, err := x.marshal()
	if err != nil {
		d.log.WithField("action", "delegate.local_state.marshal").WithError(err).
			Error("failed to marshal local state")
		return nil
	}
	return bytes
}

// MergeRemoteState is invoked after a TCP Push/Pull. This is the
// state received from the remote side and is the result of the
// remote side's LocalState call. The 'join'
// boolean indicates this is for a join instead of a push/pull.
func (d *delegate) MergeRemoteState(data []byte, join bool) {
	// Does operation match _OpCodeDisk
	if _OpCode(data[0]) != _OpCodeDisk {
		return
	}
	var x spaceMsg
	if err := x.unmarshal(data); err != nil || x.Node == "" {
		d.log.WithFields(logrus.Fields{
			"action": "delegate.merge_remote.unmarshal",
			"data":   string(data),
		}).WithError(err).Error("failed to unmarshal remote state")
		return
	}
	info := NodeInfo{DiskUsage: x.DiskUsage, LastTimeMilli: time.Now().UnixMilli(), Status: NodeStatusActive}
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
// free spaces is less than 32MB.
// The free space is just an rough estimate of the actual amount.
// The Lower bound 32MB helps to mitigate the risk of selecting same set of nodes
// when selections happens concurrently on different initiator nodes.
func (d *delegate) sortCandidates(names []string) []string {
	rand.Shuffle(len(names), func(i, j int) { names[i], names[j] = names[j], names[i] })

	d.Lock()
	defer d.Unlock()
	m := d.Cache
	sort.Slice(names, func(i, j int) bool {
		return (m[names[j]].Available >> 25) < (m[names[i]].Available >> 25)
	})

	return names
}

// updater a function which updates node information periodically
func (d *delegate) updater(period, minPeriod time.Duration, du func(path string) (DiskUsage, error)) {
	t := time.NewTicker(period)
	defer t.Stop()
	curTime := time.Now()
	for range t.C {
		if time.Since(curTime) < minPeriod { // too short
			continue // wait for next cycle to avoid overwhelming the disk
		}
		space, err := du(d.dataPath)
		if err != nil {
			d.log.WithField("action", "delegate.local_state.disk_usage").WithError(err).
				Error("disk space updater failed")
		} else {
			d.setOwnSpace(space)
		}
		curTime = time.Now()
	}
}

// SetGracefulLeave sets the graceful leave state in metadata
// This state gets broadcasted to all nodes via memberlist
func (d *delegate) SetGracefulLeave() {
	d.metadata.Status = NodeStatusShutdown
}

// markNodeGracefulLeaving marks a cached node as gracefully leaving
func (d *delegate) markNodeGracefulLeaving(nodeName string) {
	d.Lock()
	defer d.Unlock()

	if nodeInfo, exists := d.Cache[nodeName]; exists {
		nodeInfo.Status = NodeStatusShutdown
		d.Cache[nodeName] = nodeInfo
		d.log.WithField("node", nodeName).Debug("marked cached node as gracefully leaving")
	}
}

func (d *delegate) getAllCachedGraceful() []string {
	d.Lock()
	defer d.Unlock()
	graceful := make([]string, 0, len(d.Cache))
	for nodeName, nodeInfo := range d.Cache {
		if nodeInfo.Status == NodeStatusShutdown {
			graceful = append(graceful, nodeName)
		}
	}
	return graceful
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
	// Check if this is a graceful leave before deleting from cache
	if len(node.Meta) > 0 {
		var meta NodeMetadata
		if err := json.Unmarshal(node.Meta, &meta); err == nil {
			if meta.Status == NodeStatusShutdown {
				e.d.log.WithField("node", node.Name).Debug("node gracefully left, marking as gracefully leaving and keeping in cache")
				// Mark this node as gracefully leaving in the cache
				e.d.markNodeGracefulLeaving(node.Name)
				return // Don't delete gracefully leaving nodes from cache
			}
		}
	}

	// Only delete from cache if it's not a graceful leave
	e.d.log.WithField("node", node.Name).Debug("node left unexpectedly, removing from cache")
	e.d.delete(node.Name)
}

// NotifyUpdate is invoked when a node is detected to have
// updated, usually involving the meta data. The Node argument
// must not be modified.
func (e events) NotifyUpdate(*memberlist.Node) {}
