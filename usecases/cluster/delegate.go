package cluster

import (
	"bytes"
	"encoding/binary"
	"sort"
	"sync"
	"syscall"
)

// TODO: add version and opcode

type nodeSpace struct {
	Name string
	DiskSpace
}
type DiskSpace struct {
	Total     uint64
	Available uint64
}

func (d *nodeSpace) marshal() (data []byte, err error) {
	buf := bytes.NewBuffer(make([]byte, 0, 16+len(d.Name)))
	if err := binary.Write(buf, binary.BigEndian, d.DiskSpace); err != nil {
		return nil, err
	}
	_, err = buf.Write([]byte(d.Name))
	return buf.Bytes(), err
}

func (d *nodeSpace) Unmarshal(data []byte) error {
	rd := bytes.NewReader(data)
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
	DiskUsage map[string]DiskSpace
}

func (*delegate) NodeMeta(limit int) (meta []byte) {
	return nil
}

func (d *delegate) LocalState(join bool) []byte {
	// TODO should we return if join == true
	space, err := diskSpace(d.dataPath)
	if err != nil {
		return nil
	}
	d.Set(d.Name, space) // store internally
	x := nodeSpace{d.Name, space}
	bytes, err := x.marshal()
	if err != nil {
		return nil
	}
	return bytes
}

func (d *delegate) MergeRemoteState(data []byte, join bool) {
	// TODO should we return if join == true
	var x nodeSpace
	if err := x.Unmarshal(data); err != nil || x.Name == "" {
		return
	}
	d.Set(x.Name, x.DiskSpace)
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

func (d *delegate) sortCandidates(names []string) []string {
	d.Lock()
	defer d.Unlock()
	m := d.DiskUsage
	sort.Slice(names, func(i, j int) bool {
		return m[names[j]].Available < m[names[i]].Available
	})
	return names
}
