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

type nodeSpace struct {
	Name string
	DiskInfo
}

// header of an operation
type header struct {
	// OpCode operation code
	OpCode _OpCode
	// ProtoVersion protocol we will speak
	ProtoVersion uint8
}

// DiskInfo disk space
type DiskInfo struct {
	header
	// Total disk space
	Total uint64
	// Total available space
	Available uint64
}

func (d *nodeSpace) marshal() (data []byte, err error) {
	buf := bytes.NewBuffer(make([]byte, 0, 16+len(d.Name)))
	if err := binary.Write(buf, binary.BigEndian, d.DiskInfo); err != nil {
		return nil, err
	}
	_, err = buf.Write([]byte(d.Name))
	return buf.Bytes(), err
}

func (d *nodeSpace) unmarshal(data []byte) error {
	rd := bytes.NewReader(data)
	if err := binary.Read(rd, binary.BigEndian, &d.DiskInfo); err != nil {
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
	LastTime  time.Time
	DiskUsage map[string]DiskInfo
}

func (*delegate) NodeMeta(limit int) (meta []byte) {
	return nil
}

func (d *delegate) LocalState(join bool) []byte {
	// TODO should we return if join == true
	// shouldUpdate = join == true ||
	d.Lock() // get previous state
	prvTime, prvInfo := d.LastTime, d.DiskUsage[d.Name]
	d.Unlock()
	// TODO: do we need to check "join" dur < _ProtoTTL might be sufficient
	var space DiskInfo
	var err error
	// Is it time to update?
	if prvInfo.Available == 0 || time.Since(prvTime) > _ProtoTTL {
		space, err = diskSpace(d.dataPath)
	}
	// no need to update if space didn't change too much
	a, b := prvInfo.Available, space.Available
	if err != nil || !join && ((b >= a && b-a < KB) ||
		(a >= b && a-b < KB)) {
		return nil
	}

	d.Set(d.Name, space) // store internally
	x := nodeSpace{d.Name, space}
	bytes, err := x.marshal()
	if err != nil {
		return nil
	}
	d.Lock()
	d.LastTime = time.Now()
	d.Unlock()
	return bytes
}

func (d *delegate) MergeRemoteState(data []byte, join bool) {
	var x nodeSpace
	if err := x.unmarshal(data); err != nil || x.Name == "" {
		return
	}
	d.Set(x.Name, x.DiskInfo)
}

func (d *delegate) NotifyMsg(data []byte) {}

func (d *delegate) GetBroadcasts(overhead, limit int) [][]byte {
	return nil
}

func (d *delegate) Get(node string) (DiskInfo, bool) {
	d.Lock()
	defer d.Unlock()
	x, ok := d.DiskUsage[node]
	return x, ok
}

func (d *delegate) Set(node string, x DiskInfo) {
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

func diskSpace(path string) (DiskInfo, error) {
	fs := syscall.Statfs_t{}
	err := syscall.Statfs(path, &fs)
	if err != nil {
		return DiskInfo{}, err
	}
	return DiskInfo{
		header:    header{OpCode: _OpCodeDisk, ProtoVersion: _ProtoVersion},
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
