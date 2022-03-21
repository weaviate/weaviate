package db

import (
	"fmt"
	"syscall"
	"time"
)

type diskUse struct {
	total uint64
	avail uint64
}

func (d diskUse) percentUsed() float64 {
	used := d.total - d.avail
	return (float64(used) / float64(d.total)) * 100
}

func (d diskUse) String() string {
	GB := 1024 * 1024 * 1024

	return fmt.Sprintf("total: %.2fGB, avail: %.2fGB, used: %.2fGB",
		float64(d.total)/float64(GB),
		float64(d.avail)/float64(GB),
		float64(d.total-d.avail)/float64(GB))
}

func (d *DB) scanDiskUse() {
	go func() {
		t := time.Tick(time.Second * 30)
		select {
		case <-d.shutdown:
			return
		case <-t:
			d.indexLock.Lock()
			for _, i := range d.indices {
				for _, s := range i.Shards {
					diskPath := i.Config.RootPath
					du := d.getDiskUse(diskPath)

					s.diskUseWarn(du, diskPath)
					s.diskUseReadonly(du, diskPath)
				}
			}
			d.indexLock.Unlock()
		}
	}()
}

func (d *DB) getDiskUse(diskPath string) diskUse {
	fs := syscall.Statfs_t{}

	err := syscall.Statfs(diskPath, &fs)
	if err != nil {
		d.logger.WithField("action", "read_disk_use").
			WithField("path", diskPath).
			Fatalf("failed to read disk usage: %s", err)
	}

	return diskUse{
		total: fs.Blocks * uint64(fs.Bsize),
		avail: fs.Bavail * uint64(fs.Bsize),
	}
}
