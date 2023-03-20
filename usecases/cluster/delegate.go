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
)

// _OpCode represents the type of supported operation
type _OpCode uint8

const (
	_ProtoVersion uint8   = 1
	_ProtoTTL             = time.Second * 20
	_OpCodeDisk   _OpCode = 1
	KB                    = 1 << 10
)

// TODO:
// truncate total and available space to MB
// add TTL to send disk info
// cache disk space
// send disk info if join == true otherwise when TTL expires

type spaceRequest struct {
	header
	DiskSpace
	Name string
}

// header of an operation
type header struct {
	// OpCode operation code
	OpCode _OpCode
	// ProtoVersion protocol we will speak
	ProtoVersion uint8
}

type DiskSpace struct {
	// Total disk space
	Total uint64
	// Total available space
	Available uint64
}

// NodeInfo disk space
type NodeInfo struct {
	DiskSpace
	LastTime time.Time
}

func (d *spaceRequest) marshal() (data []byte, err error) {
	buf := bytes.NewBuffer(make([]byte, 0, 18+len(d.Name)))
	if err := binary.Write(buf, binary.BigEndian, d.header); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, d.DiskSpace); err != nil {
		return nil, err
	}
	_, err = buf.Write([]byte(d.Name))
	return buf.Bytes(), err
}

func (d *spaceRequest) unmarshal(data []byte) error {
	rd := bytes.NewReader(data)
	if err := binary.Read(rd, binary.BigEndian, &d.header); err != nil {
		return err
	}
	if err := binary.Read(rd, binary.BigEndian, &d.DiskSpace); err != nil {
		return err
	}
	// fmt.Println(rd.Size(), rd.Len(), len(data), string(data))
	d.Name = string(data[len(data)-rd.Len():])
	return nil
}

type delegate struct {
	Name     string
	dataPath string
	sync.Mutex
	DiskUsage map[string]NodeInfo
}

func (d *delegate) init() error {
	d.DiskUsage = make(map[string]NodeInfo, 32)
	space, err := diskSpace(d.dataPath)
	if err != nil {
		return fmt.Errorf("disk_space: %w", err)
	}

	d.Set(d.Name, NodeInfo{space, time.Now()}) // cache
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
	prv, _ := d.Get(d.Name) // value from cache
	var (
		info NodeInfo = prv
		err  error
	)
	// renew cached value if ttl expires
	if prv.Available == 0 || time.Since(prv.LastTime) > _ProtoTTL {
		info.DiskSpace, err = diskSpace(d.dataPath)
		if err != nil {
			return nil
		}
		info.LastTime = time.Now()
	}

	if !prv.LastTime.Equal(info.LastTime) {
		d.Set(d.Name, info) // cache new value
	}

	x := spaceRequest{
		header{OpCode: 1, ProtoVersion: _ProtoVersion},
		info.DiskSpace,
		d.Name,
	}
	bytes, err := x.marshal()
	if err != nil {
		return nil
	}
	return bytes
}

// MergeRemoteState is invoked after a TCP Push/Pull. This is the
// state received from the remote side and is the result of the
// remote side's LocalState call. The 'join'
// boolean indicates this is for a join instead of a push/pull.
func (d *delegate) MergeRemoteState(data []byte, join bool) {
	var x spaceRequest
	if err := x.unmarshal(data); err != nil || x.Name == "" {
		return
	}
	info := NodeInfo{x.DiskSpace, time.Now()}
	d.Set(x.Name, info)
}

func (d *delegate) NotifyMsg(data []byte) {}

func (d *delegate) GetBroadcasts(overhead, limit int) [][]byte {
	return nil
}

func (d *delegate) Get(node string) (NodeInfo, bool) {
	d.Lock()
	defer d.Unlock()
	x, ok := d.DiskUsage[node]
	return x, ok
}

func (d *delegate) Set(node string, x NodeInfo) {
	d.Lock()
	defer d.Unlock()
	d.DiskUsage[node] = x
}

func (d *delegate) Delete(node string) {
	// TODO clean up entries for node leaving the cluster
	d.Lock()
	defer d.Unlock()
	delete(d.DiskUsage, node)
}

func diskSpace(path string) (DiskSpace, error) {
	fs := syscall.Statfs_t{}
	err := syscall.Statfs(path, &fs)
	if err != nil {
		return DiskSpace{}, err
	}
	return DiskSpace{
		Total:     fs.Blocks * uint64(fs.Bsize),
		Available: fs.Bavail * uint64(fs.Bsize),
	}, nil
}

func (d *delegate) sortCandidates(names []string) []string {
	d.Lock()
	defer d.Unlock()
	m := d.DiskUsage
	sort.Slice(names, func(i, j int) bool {
		return m[names[j]].Available < m[names[i]].Available
	})
	return names
}
