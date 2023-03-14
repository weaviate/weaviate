package cluster

import (
	"bytes"
	"encoding/binary"
	"sync"
	"syscall"
)

type DiskSpace struct {
	// Version   int16
	Total     uint64
	Available uint64
}

func (d *DiskSpace) MarshalBinary() (data []byte, err error) {
	buf := bytes.NewBuffer(make([]byte, 0, 16))
	binary.Write(buf, binary.BigEndian, d)
	return buf.Bytes(), err
}

func (d *DiskSpace) UnmarshalBinary(data []byte) error {
	return binary.Read(bytes.NewReader(data), binary.BigEndian, d)
}

type delegate struct {
	dataPath string
	sync.Mutex
	DiskUsage map[string]DiskSpace
}

func (*delegate) NodeMeta(limit int) (meta []byte) {
	return nil
}

func (d *delegate) LocalState(join bool) []byte {
	return nil
}

func (d *delegate) MergeRemoteState(buf []byte, join bool) {
}

func (d *delegate) NotifyMsg(data []byte) {}

func (d *delegate) GetBroadcasts(overhead, limit int) [][]byte {
	return nil
}

func (d *delegate) Get(node string) (DiskSpace, bool) {
	d.Lock()
	defer d.Unlock()
	x, ok := d.DiskUsage[node]
	return x, ok
}

func (d *delegate) Set(node string, x DiskSpace) {
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
