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

package db

import (
	"fmt"
	"syscall"
	"time"
)

type diskUse struct {
	total uint64
	free  uint64
	avail uint64
}

func (d diskUse) percentUsed() float64 {
	used := d.total - d.free
	return (float64(used) / float64(d.total)) * 100
}

func (d diskUse) String() string {
	GB := 1024 * 1024 * 1024

	return fmt.Sprintf("total: %.2fGB, free: %.2fGB, used: %.2fGB (avail: %.2fGF)",
		float64(d.total)/float64(GB),
		float64(d.free)/float64(GB),
		float64(d.total-d.free)/float64(GB),
		float64(d.avail)/float64(GB))
}

func (d *DB) scanDiskUse() {
	go func() {
		t := time.Tick(time.Second * 30)
		for {
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
		}
	}()
}

func (d *DB) getDiskUse(diskPath string) diskUse {
	fs := syscall.Statfs_t{}

	err := syscall.Statfs(diskPath, &fs)
	if err != nil {
		d.logger.WithField("action", "read_disk_use").
			WithField("path", diskPath).
			Errorf("failed to read disk usage: %s", err)
	}

	return diskUse{
		total: fs.Blocks * uint64(fs.Bsize),
		free:  fs.Bfree * uint64(fs.Bsize),
		avail: fs.Bfree * uint64(fs.Bsize),
	}
}
